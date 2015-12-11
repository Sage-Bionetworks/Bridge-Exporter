package org.sagebionetworks.bridge.scripts;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.io.Files;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.file.FileHandle;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.PartialRow;
import org.sagebionetworks.repo.model.table.PartialRowSet;
import org.sagebionetworks.repo.model.table.Row;
import org.sagebionetworks.repo.model.table.SelectColumn;
import org.sagebionetworks.repo.model.table.TableEntity;

import org.sagebionetworks.bridge.exporter.BridgeExporterConfig;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.synapse.SynapseTableIterator;
import org.sagebionetworks.bridge.util.BridgeExporterUtil;

/**
 * Backfills the MomentInDay fields in the PD Tap, Voice, and Walk tables. This does so by
 * (a) iterating through each table looking for entries with non-blank MomentInDay
 * (b) querying each table for all entries for that health code within 20 minutes of that first entry
 * (c) updating those rows with MomentInDay, if the MomentInDay fields are blank
 *
 * Usage: PdMomentInDay [upload date] [[synapse table ID]...]
 */
public class PdMomentInDay {
    private static final int APPEND_PAGE_SIZE = 64;
    private static final String MOMENT_IN_DAY_FORMAT_JSON = "momentInDayFormat.json";
    private static final String MOMENT_IN_DAY_FORMAT_JSON_CHOICE_ANSWERS = "momentInDayFormat.json.choiceAnswers";
    private static final int TASK_REMAINING_REPORT_INTERVAL = 50;
    private static final long TWENTY_MINUTES_IN_MILLISECONDS = 20 * 60 * 1000;

    // Join column names for select clause. We have to double quote all column names, because some of them contain
    // periods.
    private static final Joiner SELECT_COLUMN_JOINER = Joiner.on("\", \"");

    // refactor this. global vars are bad
    private static final AtomicInteger numRecordsNotFound = new AtomicInteger();
    private static SynapseClient synapseClient;
    private static SynapseHelper synapseHelper;
    private static String[] tableIdArray;
    private final static Map<String, TableMetadata> tableMetadataMap = new HashMap<>();

    public static void main(String[] args) throws InterruptedException, IOException, SynapseException {
        try {
            // args
            if (args.length < 2) {
                throw new IllegalArgumentException("Usage: PdMomentInDay [upload date] [[synapse table ID]...]");
            }
            String uploadDate = args[0];
            tableIdArray = Arrays.copyOfRange(args, 1, args.length);

            // init config
            File synapseConfigFile = new File(System.getProperty("user.home") + "/bridge-synapse-exporter-config.json");
            BridgeExporterConfig config = BridgeExporterUtil.JSON_MAPPER.readValue(synapseConfigFile,
                    BridgeExporterConfig.class);

            // init synapse client
            synapseClient = new SynapseClientImpl();
            synapseClient.setUserName(config.getUsername());
            synapseClient.setApiKey(config.getApiKey());

            // init synapse helper with synapse client
            synapseHelper = new SynapseHelper();
            synapseHelper.setSynapseClient(synapseClient);

            // executor
            ExecutorService executor = Executors.newFixedThreadPool(config.getNumThreads());

            // query synapse for each table to get its parent project ID and column info
            System.out.println("Initializing table metadata...");
            for (String oneTableId : tableIdArray) {
                TableMetadata oneTableMetadata = new TableMetadata();

                // get parent ID
                TableEntity oneTable = synapseHelper.getTableWithRetry(oneTableId);
                oneTableMetadata.parentId = oneTable.getParentId();

                // get column data
                List<ColumnModel> columnList = synapseHelper.getColumnModelsForTableWithRetry(oneTableId);
                for (ColumnModel oneColumn : columnList) {
                    String columnName = oneColumn.getName();

                    // columns that start with momentInDayFormat.json are moment in day columns
                    if (columnName.startsWith(MOMENT_IN_DAY_FORMAT_JSON)) {
                        oneTableMetadata.momentInDayColumnSet.add(columnName);
                        oneTableMetadata.columnNameToId.put(columnName, oneColumn.getId());
                    }

                    // if it's one of these two, it's the column we're query keying on
                    if (columnName.equals(MOMENT_IN_DAY_FORMAT_JSON)
                            || columnName.equals(MOMENT_IN_DAY_FORMAT_JSON_CHOICE_ANSWERS)) {
                        oneTableMetadata.queryColumn = columnName;
                    }
                }

                if (Strings.isNullOrEmpty(oneTableMetadata.queryColumn)
                        || oneTableMetadata.momentInDayColumnSet.isEmpty()) {
                    throw new IllegalStateException(
                            "Synapse table " + oneTableId + " doesn't have moment in day fields");
                }

                tableMetadataMap.put(oneTableId, oneTableMetadata);
            }

            // Any table may have the momentInDay, so we need to check all of them.
            Queue<Future<?>> outstandingTaskQueue = new LinkedList<>();
            for (String oneTableId : tableIdArray) {
                System.out.println("Scanning table " + oneTableId);
                TableMetadata oneTableMetadata = tableMetadataMap.get(oneTableId);

                // find rows that are missing momentInDayFormat
                // always include recordId, healthCode, and createdOn
                String sql = "SELECT recordId, healthCode, createdOn FROM " + oneTableId + " WHERE \""
                        + oneTableMetadata.queryColumn + "\" IS NULL AND uploadDate='" + uploadDate + "'";

                // Iterate table results
                SynapseTableIterator tableIter = new SynapseTableIterator(synapseClient, sql, oneTableId);
                while (tableIter.hasNext()) {
                    Task task = new Task();
                    task.row = tableIter.next();
                    task.tableId = oneTableId;
                    Future<?> taskFuture = executor.submit(task);
                    outstandingTaskQueue.add(taskFuture);
                }
            }

            // Wait for outstanding tasks
            Stopwatch outstandingTaskStopwatch = Stopwatch.createStarted();
            while (!outstandingTaskQueue.isEmpty()) {
                int numOutstanding = outstandingTaskQueue.size();
                if (numOutstanding % TASK_REMAINING_REPORT_INTERVAL == 0) {
                    System.out.println("Num outstanding tasks: " + numOutstanding + " after " +
                            outstandingTaskStopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
                }

                // Tasks have no return value. If Future.get() returns normally, then the task is done.
                Future<?> oneFuture = outstandingTaskQueue.remove();
                try {
                    oneFuture.get();
                } catch (ExecutionException | InterruptedException ex) {
                    System.out.println("[ERROR] Error finishing task: " + ex.getMessage());
                    ex.printStackTrace(System.out);
                }
            }

            // Report
            for (String oneTableId : tableIdArray) {
                TableMetadata oneTableMetadata = tableMetadataMap.get(oneTableId);
                System.out.println("Rows to update in " + oneTableId + ": " + oneTableMetadata.rowUpdateList.size());
            }
            System.out.println("Rows that couldn't be matched: " + numRecordsNotFound.intValue());

            // Update rows
            for (String oneTableId : tableIdArray) {
                System.out.println("Updating table " + oneTableId);
                TableMetadata oneTableMetadata = tableMetadataMap.get(oneTableId);
                if (oneTableMetadata.rowUpdateList.isEmpty()) {
                    // Skip
                    continue;
                }

                int numRowsToUpdate = oneTableMetadata.rowUpdateList.size();
                for (int i = 0; i < numRowsToUpdate; i += APPEND_PAGE_SIZE) {
                    int endIdx = Math.min(numRowsToUpdate, i + APPEND_PAGE_SIZE);
                    List<TableRowUpdate> rowUpdatePage = oneTableMetadata.rowUpdateList.subList(i, endIdx);

                    List<PartialRow> partialRowList = new ArrayList<>();
                    for (TableRowUpdate oneRowUpdate : rowUpdatePage) {
                        // make partial row set
                        PartialRow partialRow = new PartialRow();
                        partialRow.setRowId(oneRowUpdate.rowId);
                        partialRow.setValues(oneRowUpdate.columnIdToValue);
                        partialRowList.add(partialRow);
                    }

                    // batch update rows
                    PartialRowSet partialRowSet = new PartialRowSet();
                    partialRowSet.setTableId(oneTableId);
                    partialRowSet.setRows(partialRowList);
                    synapseHelper.appendRowsToTableWithRetry(partialRowSet, oneTableId);
                }
            }

            System.out.println("Done");
        } catch (Throwable t) {
            t.printStackTrace(System.out);
        } finally {
            System.exit(0);
        }
    }

    // This class (really a struct) tracks table metadata needed for constructing the queries to Synapse, including
    // columns to select, columns to query on, and partial rows to be written.
    private static class TableMetadata {
        private final Map<String, String> columnNameToId = new HashMap<>();

        // List of columns that are momentInDay columns. This is used both for selecting from Synapse and as a list of
        // columns to update.
        private final Set<String> momentInDayColumnSet = new HashSet<>();

        // parent project ID
        private String parentId;

        // Column to query on, for example "WHERE [queryColumn] IS NOT NULL". This is usually
        // momentInDayFormat.json.choiceAnswers or momentInDayFormat.json
        private String queryColumn;

        // List of rows to update
        private final List<TableRowUpdate> rowUpdateList = Collections.synchronizedList(
                new ArrayList<TableRowUpdate>());
    }

    // This class (really a struct) contains data needed to update a table row.
    private static class TableRowUpdate {
        private final Map<String, String> columnIdToValue = new HashMap<>();
        private String recordId;
        private long rowId;
    }

    private static class Task implements Runnable {
        private Row row;
        private String tableId;

        @Override
        public void run() {
            List<String> columnValueList = row.getValues();
            TableMetadata tableMetadata = tableMetadataMap.get(tableId);

            // columns are, in order, recordId, healthCode, createdOn
            String recordId = columnValueList.get(0);
            String healthCode = columnValueList.get(1);
            long createdOn = Long.parseLong(columnValueList.get(2));

            try {
                // check for all records in all tables that happened within the last 20 minutes of this record, looking
                // for any row with momentInDayFormat
                Map<String, String> momentInDayColumnValueMap = new HashMap<>();
                for (String innerTableId : tableIdArray) {
                    TableMetadata innerTableMetadata = tableMetadataMap.get(innerTableId);
                    String innerSql = "SELECT \"" + SELECT_COLUMN_JOINER.join(innerTableMetadata.momentInDayColumnSet)
                            + "\" FROM " + innerTableId + " WHERE healthCode='" + healthCode + "' AND createdOn >= "
                            + (createdOn - TWENTY_MINUTES_IN_MILLISECONDS) + " AND createdOn <= " + createdOn
                            + " AND \"" + innerTableMetadata.queryColumn + "\" IS NOT NULL LIMIT 1";
                    SynapseTableIterator innerTableIter = new SynapseTableIterator(synapseClient, innerSql,
                            innerTableId);

                    if (innerTableIter.hasNext()) {
                        // We found one. Get values.
                        Row innerRow = innerTableIter.next();
                        List<String> innerColumnValueList = innerRow.getValues();
                        List<SelectColumn> innerSelectColumnList = innerTableIter.getHeaders();
                        for (int i = 0; i < innerSelectColumnList.size(); i++) {
                            String columnName = innerSelectColumnList.get(i).getName();
                            String columnValue = innerColumnValueList.get(i);
                            momentInDayColumnValueMap.put(columnName, columnValue);
                        }
                    }
                }

                if (momentInDayColumnValueMap.isEmpty()) {
                    // This is sadly all too common. Increment a counter and silently return.
                    numRecordsNotFound.incrementAndGet();
                    return;
                }

                // Figure out how we want to update this row.
                TableRowUpdate rowUpdate = new TableRowUpdate();
                rowUpdate.recordId = recordId;
                rowUpdate.rowId = row.getRowId();

                for (String oneUpdateColumn : tableMetadata.momentInDayColumnSet) {
                    String innerColumnValue = momentInDayColumnValueMap.get(oneUpdateColumn);

                    if (Strings.isNullOrEmpty(innerColumnValue)) {
                        if (oneUpdateColumn.equals(MOMENT_IN_DAY_FORMAT_JSON)) {
                            // copy it from the choice answers field
                            String choiceAnswersString = momentInDayColumnValueMap.get(
                                    MOMENT_IN_DAY_FORMAT_JSON_CHOICE_ANSWERS);
                            JsonNode choiceAnswersJson = BridgeExporterUtil.JSON_MAPPER.readTree(choiceAnswersString);

                            ObjectNode momentInDayJson = BridgeExporterUtil.JSON_MAPPER.createObjectNode();
                            momentInDayJson.set("choiceAnswers", choiceAnswersJson);
                            String momentInDayJsonString = BridgeExporterUtil.JSON_MAPPER
                                    .writerWithDefaultPrettyPrinter().writeValueAsString(momentInDayJson);
                            byte[] momentInDayJsonBytes = momentInDayJsonString.getBytes(Charsets.UTF_8);

                            // upload the file to Synapse and record the file handle ID
                            File tmpFile = File.createTempFile("momentInDayFormat", ".json");
                            Files.write(momentInDayJsonBytes, tmpFile);
                            FileHandle fileHandle = synapseHelper.createFileHandleWithRetry(tmpFile, "text/json",
                                    tableMetadata.parentId);
                            tmpFile.delete();

                            // write that file handle ID to the map of columns
                            innerColumnValue = fileHandle.getId();
                        } else if (oneUpdateColumn.equals(MOMENT_IN_DAY_FORMAT_JSON_CHOICE_ANSWERS)) {
                            // download from the momentInDayFormat.json attachment and parse out the choiceAnswers
                            String momentInDayFormatFileHandleId = momentInDayColumnValueMap.get(
                                    MOMENT_IN_DAY_FORMAT_JSON);
                            File tmpFile = File.createTempFile("momentInDayFormat", ".json");
                            synapseHelper.downloadFileHandleWithRetry(momentInDayFormatFileHandleId, tmpFile);
                            byte[] momentInDayJsonBytes = Files.toByteArray(tmpFile);
                            tmpFile.delete();

                            JsonNode momentInDayJson = BridgeExporterUtil.JSON_MAPPER.readTree(
                                    momentInDayJsonBytes);
                            innerColumnValue = momentInDayJson.get("choiceAnswers").toString();
                        }
                    }

                    String columnId = tableMetadata.columnNameToId.get(oneUpdateColumn);
                    rowUpdate.columnIdToValue.put(columnId, innerColumnValue);
                }

                tableMetadata.rowUpdateList.add(rowUpdate);
            } catch (IOException | SynapseException ex) {
                throw new RuntimeException("Error handling record " + recordId + ": " + ex.getMessage(), ex);
            }
        }
    }
}
