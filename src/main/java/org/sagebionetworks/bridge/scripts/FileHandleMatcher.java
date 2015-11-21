package org.sagebionetworks.bridge.scripts;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.sagebionetworks.repo.model.table.Row;
import org.sagebionetworks.repo.model.table.SelectColumn;
import org.sagebionetworks.repo.model.table.TableEntity;

import org.sagebionetworks.bridge.exporter.BridgeExporterConfig;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.synapse.SynapseTableIterator;
import org.sagebionetworks.bridge.util.BridgeExporterUtil;

public class FileHandleMatcher {
    private static SynapseClient synapseClient;
    private static SynapseHelper synapseHelper;
    private static Table synapseTablesDdbTable;

    public static void main(String[] args) throws IOException {
        init();
        execute(args[0]);
    }

    public static void init() throws IOException {
        // init ddb
        DynamoDB ddbClient = new DynamoDB(new AmazonDynamoDBClient());
        synapseTablesDdbTable = ddbClient.getTable("public-mpower-SynapseTables");

        // init Synapse config and client
        File synapseConfigFile = new File(System.getProperty("user.home") + "/bridge-synapse-exporter-config.json");
        BridgeExporterConfig config = BridgeExporterUtil.JSON_MAPPER.readValue(synapseConfigFile,
                BridgeExporterConfig.class);

        // synapse client
        synapseClient = new SynapseClientImpl();
        synapseClient.setUserName(config.getUsername());
        synapseClient.setApiKey(config.getApiKey());

        // synapse helper - all we need is the client
        synapseHelper = new SynapseHelper();
        synapseHelper.setSynapseClient(synapseClient);
    }

    @SuppressWarnings("ConstantConditions")
    public static void execute(String inputFileName) throws IOException {
        // Get wrong file handle IDs. We read it as a list of lines, but convert it into a hash set so we can do fast
        // checking.
        List<String> wrongFileHandleList = Files.readLines(new File(inputFileName), Charsets.UTF_8);
        Set<String> wrongFileHandleSet = ImmutableSet.copyOf(wrongFileHandleList);

        // iterate over all Synapse tables
        Iterable<Item> synapseTablesDdbIter = synapseTablesDdbTable.scan();
        for (Item oneSynapseTableDdbItem : synapseTablesDdbIter) {
            String schemaKey = oneSynapseTableDdbItem.getString("schemaKey");
            String tableId = oneSynapseTableDdbItem.getString("tableId");
            if (!schemaKey.startsWith("parkinson")) {
                // Filter out non-parkinson tables.
                System.out.println("Invalid schemaKey " + schemaKey);
                continue;
            }

            System.out.println("Checking out tableId " + tableId + ", schema " + schemaKey);

            try {
                // check if table is valid
                TableEntity table;
                try {
                    table = synapseHelper.getTableWithRetry(tableId);
                } catch (SynapseException ex) {
                    System.err.println("Could not get table " + tableId + ", schema " + schemaKey + ": " +
                            ex.getMessage());
                    continue;
                }
                if (table == null) {
                    System.err.println("Table " + tableId + ", schema " + schemaKey + " not found");
                    continue;
                }

                SynapseTableIterator tableRowIter = new SynapseTableIterator(synapseClient, "SELECT * FROM " + tableId,
                        tableId);

                // get headers
                List<SelectColumn> headerList = tableRowIter.getHeaders();
                int numCols = headerList.size();

                // check for recordId column and file handle columns
                Integer recordIdColIdx = null;
                boolean hasFileHandleCols = false;
                for (int i = 0; i < numCols; i++) {
                    SelectColumn oneHeader = headerList.get(i);

                    if ("recordId".equals(oneHeader.getName())) {
                        recordIdColIdx = i;
                    }

                    if (oneHeader.getColumnType() == ColumnType.FILEHANDLEID) {
                        hasFileHandleCols = true;
                    }
                }
                if (recordIdColIdx == null) {
                    System.err.println("Table " + tableId + ", schema " + schemaKey + " has no recordId column");
                    continue;
                }
                if (!hasFileHandleCols) {
                    // optimization: no need to search here if there are no file handle IDs
                    System.out.println("Table " + tableId + ", schema " + schemaKey + " has no file handle columns");
                    continue;
                }

                // iterate over rows
                int rowCount = 0;
                Stopwatch stopwatch = Stopwatch.createStarted();
                try {
                    while (tableRowIter.hasNext()) {
                        Row oneRow = tableRowIter.next();
                        List<String> rowValueList = oneRow.getValues();
                        String recordId = rowValueList.get(recordIdColIdx);

                        try {
                            // iterate over columns
                            for (int i = 0; i < numCols; i++) {
                                SelectColumn oneHeader = headerList.get(i);
                                if (oneHeader.getColumnType() != ColumnType.FILEHANDLEID) {
                                    // not file handle, don't care
                                    continue;
                                }

                                String fileHandleId = rowValueList.get(i);
                                if (wrongFileHandleSet.contains(fileHandleId)) {
                                    System.out.println("BAD FILEHANDLE FOUND: table " + schemaKey + " (id " + tableId +
                                            "), recordId " + recordId + ", column " + i + ", filehandleId " +
                                            fileHandleId);
                                }
                            }
                        } catch (RuntimeException ex) {
                            System.err.println("Error iterating Synapse table " + tableId + ", schema " + schemaKey +
                                    ", recordId " + recordId + ": " + ex.getMessage());
                            ex.printStackTrace();
                        }
                    }
                } finally {
                    System.out.println("Done scanning table " + tableId + ", schema " + schemaKey + ": " + rowCount +
                            " rows in " + stopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
                }
            } catch (RuntimeException | SynapseException ex) {
                System.err.println("Error processing Synapse table " + tableId + ", schema " + schemaKey + ": " +
                        ex.getMessage());
                ex.printStackTrace();
            }
        }

        System.out.println("Done");
    }
}
