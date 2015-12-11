package org.sagebionetworks.bridge.scripts;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.io.Files;
import org.joda.time.DateTime;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.table.Row;
import org.sagebionetworks.repo.model.table.RowSelection;

import org.sagebionetworks.bridge.exporter.BridgeExporterConfig;
import org.sagebionetworks.bridge.synapse.SynapseTableIterator;
import org.sagebionetworks.bridge.util.BridgeExporterUtil;

/**
 * <p>
 * This script takes a study name and a list of record IDs. It then queries the appVersion table for that record, finds
 * the original table, queries that table, and dedupdes or deletes those rows (from both the appVersion table and the
 * original table). This is to handle dupe records caused by missing attachments, where we know the duplicate record
 * IDs and we care about which one gets deleted.
 * </p>
 * <p>
 * IMPORTANT NOTE: This script assumes that you backfilled the dupe rows on a different uploadDate than the original.
 * </p>
 */
public class SynapseRecordDedupe2 {
    private static final int PROGRESS_REPORT_INTERVAL = 100;

    public static void main(String[] args) {
        // args
        if (args.length != 3) {
            throw new IllegalArgumentException("Usage: SynapseRecordDedupe2 [DEDUPE/DELETE] [studyId] [recordId file]");
        }
        Mode mode = Mode.valueOf(args[0]);
        String studyId = args[1];
        File recordIdFile = new File(args[2]);

        try {
            SynapseRecordDedupe2 dedupe = new SynapseRecordDedupe2(mode, studyId, recordIdFile);
            dedupe.init();
            dedupe.execute();
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            System.exit(0);
        }
    }

    private enum Mode {
        DEDUPE,
        DELETE,
    }

    // script input
    private final Mode mode;
    private final String studyId;
    private final File recordIdFile;

    // internal helpers and state
    private Table ddbSynapseMetaTables;
    private Table ddbSynapseTables;
    private final Map<String, String> schemaToTable = new HashMap<>();
    private final Map<String, RowSelection> rowsToDeleteByTable = new HashMap<>();
    private SynapseClient synapseClient;

    public SynapseRecordDedupe2(Mode mode, String studyId, File recordIdFile) {
        this.mode = mode;
        this.studyId = studyId;
        this.recordIdFile = recordIdFile;
    }

    public void init() throws IOException {
        // init config
        File synapseConfigFile = new File(System.getProperty("user.home") + "/bridge-synapse-exporter-config.json");
        BridgeExporterConfig config = BridgeExporterUtil.JSON_MAPPER.readValue(synapseConfigFile,
                BridgeExporterConfig.class);

        // init synapse client
        synapseClient = new SynapseClientImpl();
        synapseClient.setUserName(config.getUsername());
        synapseClient.setApiKey(config.getApiKey());

        // Dynamo DB client - move to Spring
        // This gets credentials from the default credential chain. For developer desktops, this is ~/.aws/credentials.
        // For EC2 instances, this happens transparently.
        // See http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html and
        // http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html#set-up-creds for more
        // info.
        DynamoDB ddbClient = new DynamoDB(new AmazonDynamoDBClient());
        ddbSynapseMetaTables = ddbClient.getTable(config.getDdbPrefix() + "SynapseMetaTables");
        ddbSynapseTables = ddbClient.getTable(config.getDdbPrefix() + "SynapseTables");
    }

    public void execute() throws IOException {
        Item metaTableRecord = ddbSynapseMetaTables.getItem("tableName", studyId + "-appVersion");
        String appVersionTableId = metaTableRecord.getString("tableId");
        System.out.println("Started " + DateTime.now().toString());

        // loop over record IDs
        try (BufferedReader recordIdReader = Files.newReader(recordIdFile, Charsets.UTF_8)) {
            String oneRecordId;
            int numRecords = 0;
            Stopwatch stopwatch = Stopwatch.createStarted();
            while ((oneRecordId = recordIdReader.readLine()) != null) {
                try {
                    // Get the rows from the appVersion table.
                    SynapseTableIterator appVersionTableIter = new SynapseTableIterator(synapseClient,
                            "SELECT originalTable FROM " + appVersionTableId + " WHERE recordId='" + oneRecordId +
                                    "' ORDER BY uploadDate DESC",
                            appVersionTableId);
                    List<Row> appVersionRowList = toList(appVersionTableIter);
                    if (appVersionRowList.isEmpty()) {
                        System.out.println("Record " + oneRecordId + " not found");
                    } else {
                        // Extract the original table from the first row. It *should* be the same for all rows.
                        String originalTableSchemaKey = appVersionRowList.get(0).getValues().get(0);

                        // Dedupe appVersion rows. It doesn't matter which row we delete, because the appVersion rows
                        // will be all the same.
                        markDupeRowsToDelete(appVersionTableId, appVersionTableIter.getEtag(), appVersionRowList);

                        // Get original table ID from DDB.
                        String origTableId = getTableIdForSchema(originalTableSchemaKey);

                        // Get rows from original table and dedupe.
                        SynapseTableIterator origTableIter = new SynapseTableIterator(synapseClient,
                                "SELECT recordId FROM " + origTableId + " WHERE recordId='" + oneRecordId +
                                        "' ORDER BY uploadDate DESC",
                                origTableId);
                        List<Row> origRowList = toList(origTableIter);
                        markDupeRowsToDelete(origTableId, origTableIter.getEtag(), origRowList);
                    }
                } catch (RuntimeException | SynapseException ex) {
                    System.out.println("Error processing record " + oneRecordId + ": " + ex.getMessage());
                    ex.printStackTrace();
                }

                // report regular progress
                if ((++numRecords) % PROGRESS_REPORT_INTERVAL == 0) {
                    System.out.println("Records processed: " + numRecords + " in " +
                            stopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
                }
            }

            System.out.println("Total records processed: " + numRecords + " in " +
                    stopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
        }

        // delete marked rows
        int totalRowsDeleted = 0;
        for (Map.Entry<String, RowSelection> rowsToDeleteEntry : rowsToDeleteByTable.entrySet()) {
            String tableId = rowsToDeleteEntry.getKey();
            RowSelection rowSelection = rowsToDeleteEntry.getValue();

            try {
                synapseClient.deleteRowsFromTable(rowSelection);
            } catch (RuntimeException | SynapseException ex) {
                System.out.println("Error deleting from table " + tableId + ": " + ex.getMessage());
                ex.printStackTrace();
                continue;
            }

            int rowsDeleted = rowSelection.getRowIds().size();
            totalRowsDeleted += rowsDeleted;
            System.out.println("Deleted " + rowsDeleted + " rows from table " + tableId);
        }
        System.out.println("Deleted " + totalRowsDeleted + " rows total");
        System.out.println("Done");
    }

    private List<Row> toList(SynapseTableIterator tableIter) throws SynapseException {
        List<Row> rowList = new ArrayList<>();
        while (tableIter.hasNext()) {
            rowList.add(tableIter.next());
        }
        return rowList;
    }

    private String getTableIdForSchema(String schemaKey) {
        String tableId = schemaToTable.get(schemaKey);
        if (tableId == null) {
            Item tableRecord = ddbSynapseTables.getItem("schemaKey", schemaKey);
            tableId = tableRecord.getString("tableId");
            schemaToTable.put(schemaKey, tableId);
        }
        return tableId;
    }

    private void markDupeRowsToDelete(String tableId, String etag, List<Row> rowList) {
        // If we're in DELETE mode, we want to delete all rows, so start from the first (index 0). If we're in DEDUPE
        // mode, we want to keep the first row, so start from index 1. (The way we have our queries, we always know the
        // first row is the one we want to keep.
        int startIndex = mode == Mode.DELETE ? 0 : 1;
        for (int i = startIndex; i < rowList.size(); i++) {
            markRowToDelete(tableId, etag, rowList.get(i).getRowId());
        }
    }

    private void markRowToDelete(String tableId, String etag, long rowId) {
        RowSelection rowSelection = rowsToDeleteByTable.get(tableId);
        if (rowSelection == null) {
            // init row selection
            rowSelection = new RowSelection();
            rowSelection.setEtag(etag);
            rowSelection.setTableId(tableId);
            rowSelection.setRowIds(new ArrayList<Long>());
            rowsToDeleteByTable.put(tableId, rowSelection);
        }

        rowSelection.getRowIds().add(rowId);
    }
}
