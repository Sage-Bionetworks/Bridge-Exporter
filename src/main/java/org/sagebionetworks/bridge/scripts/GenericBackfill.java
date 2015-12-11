package org.sagebionetworks.bridge.scripts;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3Client;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.sagebionetworks.util.csv.CsvNullReader;

import org.sagebionetworks.bridge.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.exporter.BridgeExporterConfig;
import org.sagebionetworks.bridge.exporter.UploadSchema;
import org.sagebionetworks.bridge.exporter.UploadSchemaHelper;
import org.sagebionetworks.bridge.exporter.UploadSchemaKey;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.util.BridgeExporterUtil;

/**
 * Usage: GenericBackfill [UPDATE/APPEND] [TSV file] [synapse table ID] [study ID] [schema ID] [schema rev]
 * [[old column name] [new column name]...]
 *
 * [UPDATE/APPEND] is UPDATE if we're writing to existing rows (in which case ROW_ID and ROW_VERSION must be present),
 * APPEND if we're writing to new rows (in which case ROW_ID and ROW_VERSION must be absent)
 *
 * TSV file contains the rows that need to be backfilled. The important rows are ROW_ID, ROW_VERSION, recordId, and
 * the other header rows common to all tables. This can be the table we're backfilling (in the case of backfilling
 * missing data), or it could be a different table entirely (in the case of Asthma Weekly Survey wrongfully tagged as
 * Asthma Daily Survey)
 *
 * synapse table ID is the table to write to
 *
 * study ID, schema ID, and schema rev are the schema of the table to write to
 *
 * old column name and new column name are for renaming columns, such as backfilling a column submitted with the wrong
 * name (QoL slider).
 */
public class GenericBackfill {
    private static final Joiner COLUMN_JOINER = Joiner.on('\t').useForNull("");
    private static final int NUM_COMMON_COLUMNS = 7;

    // Script should report progress after this many records, so users tailing the logs can see that it's still
    // making progress
    private static final int PROGRESS_REPORT_PERIOD = 1000;
    private static final int TASK_REMAINING_REPORT_PERIOD = 250;

    // inputs
    private Mode mode;
    private String filename;
    private String synapseTableId;
    private String studyId;
    private UploadSchemaKey schemaKey;
    private Map<String, String> newColumnToOldColumn;

    // internal state
    private BridgeExporterConfig config;
    private int lineCount = 0;
    private File newTsvFile;
    private PrintWriter newTsvWriter;
    private int numCommonAndRowVersionColumns;
    private int numSchemaColumns;
    private int numTotalColumns;
    private CsvNullReader oldFileCsvReader;
    private Integer recordIdIndex;
    private UploadSchema schema;

    // services
    private ExecutorService executor;
    private Table recordTable;
    private SynapseHelper synapseHelper;

    public static void main(String[] args) {
        if (args.length < 6 || args.length % 2 != 0) {
            throw new IllegalArgumentException("Usage: GenericBackfill [TSV file] [synapse table ID] [study ID] " +
                    "[schema ID] [schema rev] [[old column name] [new column name]...]");
        }

        GenericBackfill backfill = new GenericBackfill();
        backfill.setMode(Mode.valueOf(args[0]));
        backfill.setFilename(args[1]);
        backfill.setSynapseTableId(args[2]);
        String studyId = args[3];
        backfill.setStudyId(studyId);
        backfill.setSchemaKey(new UploadSchemaKey(studyId, args[4], Integer.parseInt(args[5])));

        Map<String, String> newColumnToOldColumn = new HashMap<>();
        for (int i = 6; i < args.length; i += 2) {
            newColumnToOldColumn.put(args[i + 1], args[i]);
        }
        backfill.setNewColumnToOldColumn(newColumnToOldColumn);

        backfill.run();
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public void setSynapseTableId(String synapseTableId) {
        this.synapseTableId = synapseTableId;
    }

    public void setStudyId(String studyId) {
        this.studyId = studyId;
    }

    public void setSchemaKey(UploadSchemaKey schemaKey) {
        this.schemaKey = schemaKey;
    }

    public void setNewColumnToOldColumn(Map<String, String> newColumnToOldColumn) {
        this.newColumnToOldColumn = newColumnToOldColumn;
    }

    private enum Mode {
        UPDATE,
        APPEND,
    }

    public void run() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            System.out.println("[METRICS] Starting initialization: " + BridgeExporterUtil.getCurrentLocalTimestamp());
            init();
            System.out.println("[METRICS] Initialization done: " + BridgeExporterUtil.getCurrentLocalTimestamp());

            backfill();
            uploadTsv();
        } catch (Throwable t) {
            // shift err into out, in case we forget to do 2&>1
            t.printStackTrace(System.out);
        } finally {
            stopwatch.stop();
            System.out.println("[METRICS] Wrote " + lineCount + " lines");
            System.out.println("[METRICS] Backfill done: " + BridgeExporterUtil.getCurrentLocalTimestamp());
            System.out.println("[METRICS] Total time: " + stopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
            System.exit(0);
        }
    }

    private void init() throws BridgeExporterException, IOException, SchemaNotFoundException, SynapseException {
        File synapseConfigFile = new File(System.getProperty("user.home") + "/bridge-synapse-exporter-config.json");
        config = BridgeExporterUtil.JSON_MAPPER.readValue(synapseConfigFile, BridgeExporterConfig.class);

        // parse file and get indices from headers
        Reader fileReader = new InputStreamReader(new FileInputStream(filename), Charsets.UTF_8);
        oldFileCsvReader = new CsvNullReader(fileReader, '\t');
        String[] oldHeaders = oldFileCsvReader.readNext();

        boolean hasRowId = false;
        boolean hasRowVersion = false;
        for (int i = 0; i < oldHeaders.length; i++) {
            String oneHeader = oldHeaders[i];
            switch (oneHeader) {
                case "recordId":
                    recordIdIndex = i;
                    break;
                case "ROW_ID":
                    hasRowId = true;
                    break;
                case "ROW_VERSION":
                    hasRowVersion = true;
                    break;
                default:
                    // we don't care
                    break;
            }
        }
        if (recordIdIndex == null) {
            throw new BridgeExporterException("No recordId column in input file");
        }
        if (mode == Mode.UPDATE) {
            if (!hasRowId) {
                throw new BridgeExporterException("ROW_ID column must be present");
            }
            if (!hasRowVersion) {
                throw new BridgeExporterException("ROW_VERSION column must be present");
            }
        } else {
            if (hasRowId) {
                throw new BridgeExporterException("ROW_ID column must be absent");
            }
            if (hasRowVersion) {
                throw new BridgeExporterException("ROW_VERSION column must be absent");
            }
        }

        // new TSV file and writer
        newTsvFile = File.createTempFile(filename + "-backfilled", ".tsv");
        OutputStream stream = new FileOutputStream(newTsvFile);
        newTsvWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(stream, Charsets.UTF_8)));

        // Dynamo DB client - move to Spring
        // This gets credentials from the default credential chain. For developer desktops, this is ~/.aws/credentials.
        // For EC2 instances, this happens transparently.
        // See http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html and
        // http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html#set-up-creds for more
        // info.
        DynamoDB ddbClient = new DynamoDB(new AmazonDynamoDBClient());
        recordTable = ddbClient.getTable(config.getBridgeDataDdbPrefix() + "HealthDataRecord3");

        // schema Helper
        UploadSchemaHelper schemaHelper = new UploadSchemaHelper();
        schemaHelper.setConfig(config);
        schemaHelper.setDdbClient(ddbClient);
        schemaHelper.init();
        schema = schemaHelper.getSchema(schemaKey);

        // num common and row version columns based on mode
        if (mode == Mode.UPDATE) {
            // 2 extra columns from ROW_ID and ROW_VERSION
            numCommonAndRowVersionColumns = 2 + NUM_COMMON_COLUMNS;
        } else {
            numCommonAndRowVersionColumns = NUM_COMMON_COLUMNS;
        }

        // num schema columns and num total columns
        numSchemaColumns = schema.getFieldNameList().size();
        numTotalColumns = numCommonAndRowVersionColumns + numSchemaColumns;

        // Write headers. First 9 columns (7 on append) are the same.
        String[] newHeaders = new String[numTotalColumns];
        System.arraycopy(oldHeaders, 0, newHeaders, 0, numCommonAndRowVersionColumns);
        for (int i = 0; i < numSchemaColumns; i++) {
            newHeaders[numCommonAndRowVersionColumns + i] = schema.getFieldNameList().get(i);
        }
        newTsvWriter.println(COLUMN_JOINER.join(newHeaders));

        // executor
        executor = Executors.newFixedThreadPool(config.getNumThreads());

        // S3 client - move to Spring
        AmazonS3Client s3Client = new AmazonS3Client();
        S3Helper s3Helper = new S3Helper();
        s3Helper.setS3Client(s3Client);

        // synapse client
        SynapseClient synapseClient = new SynapseClientImpl();
        synapseClient.setUserName(config.getUsername());
        synapseClient.setApiKey(config.getApiKey());

        // synapse helper
        synapseHelper = new SynapseHelper();
        synapseHelper.setBridgeExporterConfig(config);
        synapseHelper.setS3Helper(s3Helper);
        synapseHelper.setSynapseClient(synapseClient);
    }

    private void backfill() throws IOException {
        // Loop over old lines.
        // First line is the header, so discard
        Stopwatch submitTaskStopwatch = Stopwatch.createStarted();
        int ddbDelay = config.getDdbDelay();
        Queue<Future<?>> outstandingTaskQueue = new LinkedList<>();

        String[] columns;
        int lineNum = 0;
        while ((columns = oldFileCsvReader.readNext()) != null) {
            if (ddbDelay > 0) {
                // sleep to rate limit our requests to DDB
                try {
                    Thread.sleep(ddbDelay);
                } catch (InterruptedException ex) {
                    System.out.println("[ERROR] Interrupted while sleeping: " + ex.getMessage());
                    ex.printStackTrace(System.out);
                }
            }

            lineNum++;
            Future<?> taskFuture = executor.submit(new Task(lineNum, columns));
            outstandingTaskQueue.add(taskFuture);

            if (lineNum % PROGRESS_REPORT_PERIOD == 0) {
                System.out.println("[METRICS] Num records so far: " + lineNum + " in "
                        + submitTaskStopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
            }
        }

        // wait on outstanding tasks
        System.out.println("[METRICS] Done submitting tasks: " + BridgeExporterUtil.getCurrentLocalTimestamp());

        Stopwatch outstandingTaskStopwatch = Stopwatch.createStarted();
        while (!outstandingTaskQueue.isEmpty()) {
            int numOutstanding = outstandingTaskQueue.size();
            if (numOutstanding % TASK_REMAINING_REPORT_PERIOD == 0) {
                System.out.println("[METRICS] Num outstanding tasks: " + numOutstanding + " after " +
                        outstandingTaskStopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
            }

            // ExportWorkers have no return value. If Future.get() returns normally, then the task is done.
            Future<?> oneFuture = outstandingTaskQueue.remove();
            try {
                oneFuture.get();
            } catch (ExecutionException | InterruptedException ex) {
                System.out.println("[ERROR] Error finishing task: " + ex.getMessage());
                ex.printStackTrace(System.out);
            }
        }

        System.out.println("[METRICS] All tasks done: " + BridgeExporterUtil.getCurrentLocalTimestamp());
    }

    private class Task implements Runnable {
        private final int lineNum;
        private final String[] columns;

        public Task(int lineNum, String[] columns) {
            this.lineNum = lineNum;
            this.columns = columns;
        }

        @Override
        public void run() {
            try {
                processRecord(columns);
            } catch (Throwable t) {
                throw new RuntimeException("Error processing line " + lineNum + ": " + t.getMessage(), t);
            }
        }
    }

    private void processRecord(String[] oldColumns) throws BridgeExporterException, IOException {
        // get record
        String recordId = oldColumns[recordIdIndex];
        Item record = recordTable.getItem("id", recordId);

        // convert record to health data node
        JsonNode dataNode = BridgeExporterUtil.JSON_MAPPER.readTree(record.getString("data"));

        // Make new columns. First 9 (7 on append) are the same)
        String[] newColumns = new String[numTotalColumns];
        System.arraycopy(oldColumns, 0, newColumns, 0, numCommonAndRowVersionColumns);
        for (int i = 0; i < numSchemaColumns; i++) {
            String columnName = schema.getFieldNameList().get(i);
            String bridgeType = schema.getFieldTypeMap().get(columnName);

            JsonNode columnValueNode = dataNode.get(columnName);
            if (columnValueNode == null || columnValueNode.isNull()) {
                // fall back to old column name
                columnValueNode = dataNode.get(newColumnToOldColumn.get(columnName));
            }

            String value = synapseHelper.serializeToSynapseType(studyId, recordId, columnName, bridgeType,
                    columnValueNode);
            newColumns[numCommonAndRowVersionColumns + i] = value;
        }

        // Write the new row back to the TSV writer. We need to synchronize on the TSV writer to make sure we don't
        // conflict with other threads
        String newRow = COLUMN_JOINER.join(newColumns);
        synchronized (newTsvWriter) {
            newTsvWriter.println(newRow);
            lineCount++;
        }
    }

    private void uploadTsv() throws BridgeExporterException {
        // flush and close writer, check for errors
        newTsvWriter.flush();
        if (newTsvWriter.checkError()) {
            throw new BridgeExporterException("TSV writer has unknown error");
        }
        newTsvWriter.close();

        // filter on line count
        if (lineCount == 0) {
            // delete files with no lines--they're useless anyway
            newTsvFile.delete();
            throw new BridgeExporterException("TSV writer wrote zero lines");
        }

        // get the table, so we can figure out where to upload the file handle to
        TableEntity table;
        try {
            table = synapseHelper.getTableWithRetry(synapseTableId);
        } catch (SynapseException ex) {
            throw new BridgeExporterException("Error fetching table " + synapseTableId + " for file " +
                    newTsvFile.getAbsolutePath() + ": " + ex.getMessage(), ex);
        }
        String projectId = table.getParentId();

        long linesProcessed = synapseHelper.uploadTsvFileToTable(projectId, synapseTableId, newTsvFile);
        if (linesProcessed != lineCount) {
            throw new BridgeExporterException("Wrong number of lines processed importing to table " + synapseTableId +
                    ", expected=" + lineCount + ", actual=" + linesProcessed);
        }

        // We've successfully uploaded the file. We can delete the file now.
        newTsvFile.delete();
    }
}
