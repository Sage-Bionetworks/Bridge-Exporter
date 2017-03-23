package org.sagebionetworks.bridge.exporter.worker;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Resource;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Stopwatch;
import com.google.gson.JsonParseException;
import com.google.gson.stream.MalformedJsonException;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.dynamo.DynamoHelper;
import org.sagebionetworks.bridge.exporter.dynamo.StudyInfo;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterNonRetryableException;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterTsvException;
import org.sagebionetworks.bridge.exporter.exceptions.RestartBridgeExporterException;
import org.sagebionetworks.bridge.exporter.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.exporter.handler.AppVersionExportHandler;
import org.sagebionetworks.bridge.exporter.handler.ExportHandler;
import org.sagebionetworks.bridge.exporter.handler.HealthDataExportHandler;
import org.sagebionetworks.bridge.exporter.handler.IosSurveyExportHandler;
import org.sagebionetworks.bridge.exporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.exporter.helper.ExportHelper;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.synapse.ColumnDefinition;
import org.sagebionetworks.bridge.exporter.synapse.SynapseHelper;
import org.sagebionetworks.bridge.exporter.synapse.SynapseStatusTableHelper;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.exceptions.BridgeSDKException;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;
import org.sagebionetworks.bridge.sqs.SqsHelper;

/**
 * This class manages export handlers and workers. This includes holding the config and helper objects that the
 * handlers need, routing requests to the right handler, and handling "end of stream" events.
 */
@Component
public class ExportWorkerManager {
    private static final Logger LOG = LoggerFactory.getLogger(ExportWorkerManager.class);

    // Public, so they can be accessed in handler unit tests.
    public static final String CONFIG_KEY_EXPORTER_DDB_PREFIX = "exporter.ddb.prefix";
    public static final String CONFIG_KEY_REDRIVE_MAX_COUNT = "redrive.max.count";
    public static final String CONFIG_KEY_SYNAPSE_PRINCIPAL_ID = "synapse.principal.id";
    public static final String CONFIG_KEY_WORKER_MANAGER_PROGRESS_REPORT_PERIOD =
            "worker.manager.progress.report.period";

    // package-scoped, to be available in tests
    static final String DDB_KEY_TABLE_ID = "tableId";
    static final String REDRIVE_TAG_PREFIX = "redrive export; original: ";
    static final String SCHEMA_IOS_SURVEY = "ios-survey";

    // We need to delay our redrives. Otherwise, if we have a deterministic error, this may cause the Exporter to spin
    // as fast as possible retrying the request.
    // NOTE: This maxes out at 900 seconds (15 min) before SQS throws an error.
    static final int REDRIVE_DELAY_SECONDS = 900;

    // CONFIG

    private String exporterDdbPrefix;
    private int progressReportPeriod;
    private String recordIdOverrideBucket;
    private int redriveMaxCount;
    private long synapsePrincipalId;
    private String sqsQueueUrl;

    /** Bridge config. */
    @Autowired
    public final void setConfig(Config config) {
        this.exporterDdbPrefix = config.get(CONFIG_KEY_EXPORTER_DDB_PREFIX);
        this.recordIdOverrideBucket = config.get(BridgeExporterUtil.CONFIG_KEY_RECORD_ID_OVERRIDE_BUCKET);
        this.redriveMaxCount = config.getInt(CONFIG_KEY_REDRIVE_MAX_COUNT);
        this.synapsePrincipalId = config.getInt(CONFIG_KEY_SYNAPSE_PRINCIPAL_ID);
        this.sqsQueueUrl = config.get(BridgeExporterUtil.CONFIG_KEY_SQS_QUEUE_URL);

        this.progressReportPeriod = config.getInt(CONFIG_KEY_WORKER_MANAGER_PROGRESS_REPORT_PERIOD);
        if (progressReportPeriod == 0) {
            // Avoid mod by zero. Set to some reasonable hard-coded default.
            progressReportPeriod = 250;
        }
    }

    /**
     * The prefix for DynamoDB tables for mapping Synapse tables. Examples: "prod-exporter-". This checks for overrides
     * in the task's request before falling back to the globally configured prefix.
     *
     * @param task
     *         export task, which may or may not have the DDB prefix override
     * @return resolved DDB prefix override
     */
    public String getExporterDdbPrefixForTask(ExportTask task) {
        String override = task.getRequest().getExporterDdbPrefixOverride();
        if (StringUtils.isNotBlank(override)) {
            return override;
        }

        return exporterDdbPrefix;
    }

    /** The Synapse principal ID (user ID) for the Bridge Exporter user. */
    public long getSynapsePrincipalId() {
        return synapsePrincipalId;
    }

    // DYNAMO DB HELPERS AND OVERRIDES

    /**
     * Gets the Synapse table ID, using the DDB Synapse table map. Returns null if the Synapse table doesn't exist (no
     * entry in the DDB table).
     *
     * @param task
     *         the export task, which contains context needed to get the Synapse table map
     * @param ddbTableName
     *         Dynamo DB table that contains the Synapse table map
     * @param ddbKeyName
     *         hash key name of the Dynamo DB table
     * @param ddbKeyValue
     *         value of the hash key of the Dynamo DB table (generally the Synapse table name)
     * @return Synapse table ID of the table, or null if it doesn't exist
     */
    public String getSynapseTableIdFromDdb(ExportTask task, String ddbTableName, String ddbKeyName,
            String ddbKeyValue) {
        Table synapseTableMap = getSynapseDdbTable(task, ddbTableName);
        Item tableMapItem = synapseTableMap.getItem(ddbKeyName, ddbKeyValue);
        if (tableMapItem != null) {
            return tableMapItem.getString(DDB_KEY_TABLE_ID);
        } else {
            return null;
        }
    }

    /**
     * Writes the Synapse table ID back to the DDB Synapse table map. This is called at the end of Synapse table
     * creation.
     *
     * @param task
     *         the export task, which contains context needed to get the Synapse table map
     * @param ddbTableName
     *         Dynamo DB table that contains the Synapse table map
     * @param ddbKeyName
     *         hash key name of the Dynamo DB table
     * @param ddbKeyValue
     *         value of the hash key of the Dynamo DB table (generally the Synapse table name)
     * @param synapseTableId
     *         Synapse table ID to write to Dynamo DB
     */
    public void setSynapseTableIdToDdb(ExportTask task, String ddbTableName, String ddbKeyName, String ddbKeyValue,
            String synapseTableId) {
        Table synapseTableMap = getSynapseDdbTable(task, ddbTableName);
        Item synapseTableNewItem = new Item();
        synapseTableNewItem.withString(ddbKeyName, ddbKeyValue);
        synapseTableNewItem.withString(DDB_KEY_TABLE_ID, synapseTableId);
        synapseTableMap.putItem(synapseTableNewItem);
    }

    // Helper method to get the DDB Synapse table map, called both to read and write the Synapse table ID to and from
    // DDB.
    private Table getSynapseDdbTable(ExportTask task, String ddbTableName) {
        String ddbPrefix = getExporterDdbPrefixForTask(task);
        return ddbClient.getTable(ddbPrefix + ddbTableName);
    }

    // HELPER OBJECTS (CONFIGURED BY SPRING)

    private BridgeHelper bridgeHelper;
    private DynamoDB ddbClient;
    private DynamoHelper dynamoHelper;
    private ExportHelper exportHelper;
    private FileHelper fileHelper;
    private S3Helper s3Helper;
    private SqsHelper sqsHelper;
    private SynapseHelper synapseHelper;
    private SynapseStatusTableHelper synapseStatusTableHelper;

    // column definition list from conf file
    private List<ColumnDefinition> columnDefinitions;

    /** BridgeHelper, calls Bridge to get schemas and other data the exporter needs. */
    public final BridgeHelper getBridgeHelper() {
        return bridgeHelper;
    }

    @Resource(name = "synapseColumnDefinitions")
    public final void setSynapseColumnDefinitions(List<ColumnDefinition> columnDefinitions) {
        this.columnDefinitions = columnDefinitions;
    }

    public final List<ColumnDefinition> getColumnDefinitions() {
        return this.columnDefinitions;
    }

    /** @see #getBridgeHelper */
    @Autowired
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    /** DDB client, used to get the Synapse table mappings. */
    @Autowired
    public final void setDdbClient(DynamoDB ddbClient) {
        this.ddbClient = ddbClient;
    }

    /** DynamoHelper, used to get study info. */
    @Autowired
    public final void setDynamoHelper(DynamoHelper dynamoHelper) {
        this.dynamoHelper = dynamoHelper;
    }

    /** Export helper, used to handle attachments. */
    public final ExportHelper getExportHelper() {
        return exportHelper;
    }

    /** @see #getExportHelper */
    @Autowired
    public final void setExportHelper(ExportHelper exportHelper) {
        this.exportHelper = exportHelper;
    }

    /** File helper, used to create and write to TSVs in the file system. */
    public final FileHelper getFileHelper() {
        return fileHelper;
    }

    /** @see #getFileHelper */
    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    /** S3 Helper, used to upload list of record IDs to redrive. */
    @Autowired
    public final void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    /** Synapse Helper, used for creating Synapse tables and uploading data to Synapse tables. */
    public final SynapseHelper getSynapseHelper() {
        return synapseHelper;
    }

    /** @see #getSynapseHelper */
    @Autowired
    public final void setSynapseHelper(SynapseHelper synapseHelper) {
        this.synapseHelper = synapseHelper;
    }

    /** Specialized helper to create and write to the Status table in Synapse. */
    @Autowired
    final void setSynapseStatusTableHelper(SynapseStatusTableHelper synapseStatusTableHelper) {
        this.synapseStatusTableHelper = synapseStatusTableHelper;
    }

    /** SQS Helper, used for sending redrives back to the SQS queue. */
    @Autowired
    public final void setSqsHelper(SqsHelper sqsHelper) {
        this.sqsHelper = sqsHelper;
    }

    // STUDY INFO AND OVERRIDES

    /**
     * Convenience method for getting the access team ID for a given study. The access team ID is the team ID that is
     * allowed to read the exported data in Synapse. This calls through to DynamoDB and is present to maintain
     * backwards-compatibility and ease of migration.
     *
     * @param studyId
     *         study ID to get the access team ID for
     * @return access team ID
     */
    public long getDataAccessTeamIdForStudy(String studyId) {
        StudyInfo studyInfo = dynamoHelper.getStudyInfo(studyId);
        return studyInfo.getDataAccessTeamId();
    }

    /**
     * The Synapse project ID to export to. This first checks the task's request for the project override, then falls
     * back to Dynamo DB.
     *
     * @param studyId
     *         study ID to get the Synapse project ID for
     * @param task
     *         export task, which may or may not have a project ID override
     * @return project ID to export to
     */
    public String getSynapseProjectIdForStudyAndTask(String studyId, ExportTask task) {
        // check the override map
        Map<String, String> overrideMap = task.getRequest().getSynapseProjectOverrideMap();
        if (overrideMap != null && overrideMap.containsKey(studyId)) {
            return overrideMap.get(studyId);
        }

        // No override map. Fall back to study config (which is cached).
        StudyInfo studyInfo = dynamoHelper.getStudyInfo(studyId);
        return studyInfo.getSynapseProjectId();
    }

    // TASK AND HANDLER MANAGEMENT

    private final Map<String, AppVersionExportHandler> appVersionHandlersByStudy = new HashMap<>();
    private ExecutorService executor;
    private final Map<UploadSchemaKey, HealthDataExportHandler> healthDataHandlersBySchema = new HashMap<>();
    private final Map<String, IosSurveyExportHandler> surveyHandlersByStudy = new HashMap<>();

    /** Executor that runs our export workers. */
    @Resource(name = "workerExecutorService")
    public final void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    /**
     * Given the export task and one of the health data records in that task, this creates the export sub-tasks and
     * routes them to the appropriate export handlers. This returns immediately and queues up asynchronous workers to
     * handle those sub-tasks.
     *
     * @param task
     *         export task to be processed
     * @param record
     *         health data record within that export task to be processed
     * @throws IOException
     *         if reading the record data fails
     * @throws SchemaNotFoundException
     *         if the schema corresponding the record can't be found
     */
    public void addSubtaskForRecord(ExportTask task, Item record) throws IOException, SchemaNotFoundException {
        UploadSchemaKey schemaKey = BridgeExporterUtil.getSchemaKeyForRecord(record);
        String studyId = schemaKey.getStudyId();

        // Book-keeping: We need to know what study IDs this task has seen.
        task.addStudyId(studyId);

        // Make subtask. Subtasks are immutable, so we can safely use the same one for each of the handlers.
        JsonNode recordDataNode = DefaultObjectMapper.INSTANCE.readTree(record.getString("data"));
        ExportSubtask subtask = new ExportSubtask.Builder().withOriginalRecord(record).withParentTask(task)
                .withRecordData(recordDataNode).withSchemaKey(schemaKey).build();

        // Multiplex on schema.
        if (SCHEMA_IOS_SURVEY.equals(schemaKey.getSchemaId())) {
            // Special case: In the olden days, iOS surveys were processed by the Exporter instead of Bridge Server
            // Upload Validation. We don't do this anymore, but sometimes we want to re-export old uploads, so we still
            // need to handle this case.
            IosSurveyExportHandler surveyHandler = getSurveyHandlerForStudy(studyId);
            queueWorker(surveyHandler, task, subtask);
        } else {
            addHealthDataSubtask(task, studyId, schemaKey, subtask);
        }
    }

    /**
     * Add a health data sub-task. This is actually two sub-tasks, one for the app version table, one for the health
     * data table. This is a separate method, because the IosSurveyExportHandler needs to call this directly.
     *
     * @param parentTask
     *         parent export task that contains this health data sub-task
     * @param studyId
     *         study ID for this sub-task
     * @param schemaKey
     *         schema key for this sub-task
     * @param subtask
     *         object representing the sub-task
     * @throws IOException
     *         if getting the schema for the schema key fails
     * @throws SchemaNotFoundException
     *         if getting the schema for the schema key fails
     */
    public void addHealthDataSubtask(ExportTask parentTask, String studyId, UploadSchemaKey schemaKey,
            ExportSubtask subtask) throws IOException, SchemaNotFoundException {
        AppVersionExportHandler appVersionHandler = getAppVersionHandlerForStudy(studyId);
        queueWorker(appVersionHandler, parentTask, subtask);

        HealthDataExportHandler healthDataHandler = getHealthDataHandlerForSchema(parentTask.getMetrics(), schemaKey);
        queueWorker(healthDataHandler, parentTask, subtask);
    }

    /**
     * Adds the given task to the task queue.
     *
     * @param handler
     *         handler to queue up
     * @param parentTask
     *         parent export task, contains the task queue
     * @param subtask
     *         sub-task to queue up
     */
    private void queueWorker(ExportHandler handler, ExportTask parentTask, ExportSubtask subtask) {
        ExportWorker worker = new ExportWorker(handler, subtask);
        Future<Void> future = executor.submit(worker);
        parentTask.addSubtaskFuture(new ExportSubtaskFuture.Builder().withSubtask(subtask).withFuture(future).build());
    }

    /**
     * Gets the app version handler for the given study, with caching logic.
     *
     * @param studyId
     *         study ID for the app version handler
     * @return app version handler
     */
    private AppVersionExportHandler getAppVersionHandlerForStudy(String studyId) {
        AppVersionExportHandler handler = appVersionHandlersByStudy.get(studyId);
        if (handler == null) {
            handler = createAppVersionHandler(studyId);
            appVersionHandlersByStudy.put(studyId, handler);
        }
        return handler;
    }

    // Factory method for creating a new app version handler. This exists and is package-scoped to enable unit tests.
    AppVersionExportHandler createAppVersionHandler(String studyId) {
        AppVersionExportHandler handler = new AppVersionExportHandler();
        handler.setManager(this);
        handler.setStudyId(studyId);
        return handler;
    }

    /**
     * Gets the health data handler for the given schema key, with caching logic
     *
     * @param metrics
     *         metrics, used to track schema not found metrics
     * @param schemaKey
     *         schema for the health data handler
     * @return health data handler
     * @throws IOException
     *         if getting the schema fails
     * @throws SchemaNotFoundException
     *         if getting the schema fails
     */
    private HealthDataExportHandler getHealthDataHandlerForSchema(Metrics metrics, UploadSchemaKey schemaKey)
            throws IOException, SchemaNotFoundException {
        HealthDataExportHandler handler = healthDataHandlersBySchema.get(schemaKey);
        if (handler == null) {
            handler = createHealthDataHandler(metrics, schemaKey);
            healthDataHandlersBySchema.put(schemaKey, handler);
        }
        return handler;
    }

    // Factory method for creating a new health data handler. This exists and is package-scoped to enable unit tests.
    HealthDataExportHandler createHealthDataHandler(Metrics metrics, UploadSchemaKey schemaKey)
            throws IOException, SchemaNotFoundException {
        // Validate schema exists. This throws if schema doesn't exist. It's also cached, so we don't need to worry
        // about excessive calls to Bridge.
        bridgeHelper.getSchema(metrics, schemaKey);

        // create and return handler
        HealthDataExportHandler handler = new HealthDataExportHandler();
        handler.setManager(this);
        handler.setSchemaKey(schemaKey);
        handler.setStudyId(schemaKey.getStudyId());
        return handler;
    }

    /**
     * Gets the legacy survey handler for the given study, with caching logic.
     *
     * @param studyId
     *         study ID to get the handler for
     * @return legacy survey handler
     */
    private IosSurveyExportHandler getSurveyHandlerForStudy(String studyId) {
        IosSurveyExportHandler handler = surveyHandlersByStudy.get(studyId);
        if (handler == null) {
            handler = new IosSurveyExportHandler();
            handler.setManager(this);
            handler.setStudyId(studyId);
            surveyHandlersByStudy.put(studyId, handler);
        }
        return handler;
    }

    /**
     * Signals the end of the record stream for the given export task. This waits for all of the outstanding tasks to
     * complete and signals the handlers to upload their TSVs to Synapse.
     *
     * @param task
     *         export task to be finished
     */
    public void endOfStream(ExportTask task) throws RestartBridgeExporterException {
        BridgeExporterRequest request = task.getRequest();
        int redriveCount = request.getRedriveCount();
        String tag = request.getTag();
        LOG.info("End of stream signaled for request " + request.toString());

        // Wait for all outstanding tasks to complete
        Stopwatch stopwatch = Stopwatch.createStarted();
        Queue<ExportSubtaskFuture> subtaskFutureQueue = task.getSubtaskFutureQueue();
        Set<String> redriveRecordIdSet = new HashSet<>();
        while (!subtaskFutureQueue.isEmpty()) {
            int numOutstanding = subtaskFutureQueue.size();
            if (numOutstanding % progressReportPeriod == 0) {
                LOG.info("Num outstanding tasks: " + numOutstanding + " after " + stopwatch.elapsed(TimeUnit.SECONDS) +
                        " seconds");
            }

            // ExportWorkers have no return value. If Future.get() returns normally, then the task is done.
            ExportSubtaskFuture subtaskFuture = subtaskFutureQueue.remove();
            try {
                subtaskFuture.getFuture().get();
            } catch (ExecutionException | InterruptedException ex) {
                // The real exception is in the inner exception (if it's an ExecutionException).
                Throwable originalEx = ex.getCause();

                String recordId = subtaskFuture.getSubtask().getRecordId();
                if (isSynapseDown(originalEx)) {
                    // If Synapse is down, we should restart the BridgeEX request. Note that since BridgeEX is
                    // multi-threaded, there may be other subtasks scheduled that will run to completion. Nothing will
                    // get written to the Synapse tables, however, since (a) we never call upload to Synapse and
                    // (b) Synapse is down anyway.
                    throw new RestartBridgeExporterException("Restarting Bridge Exporter; last recordId=" + recordId +
                            ": " + originalEx.getMessage(), originalEx);
                } else {
                    LOG.error("Error completing subtask for recordId=" + recordId + ": " + ex.getMessage(), ex);
                    // We exclude TSV exceptions here. Since TSVs cause the whole table to fail, redrive the table
                    // instead of individual records.
                    if (!(originalEx instanceof BridgeExporterTsvException) && isRetryable(originalEx)) {
                        // This failure is recoverable. Track which record IDs need to be redriven, so we can redrive it
                        // later.
                        redriveRecordIdSet.add(recordId);
                    }
                }
            }
        }
        if (!redriveRecordIdSet.isEmpty() && redriveCount < redriveMaxCount) {
            // Upload the list of record IDs that need to be redriven to S3. The filename *should* be unique, since we
            // use the timestamp for the filename, and we currently only run one Export job at a time.
            // Use UTC timezone so we can easily sort and search for files. Redrives should be relatively rare, so
            // performance considerations on S3 buckets aren't an issue.
            String filename = "redrive-record-ids." + DateTime.now().withZone(DateTimeZone.UTC).toString();

            // Create a copy of the original request, except add the record override and update the tag. Also, clear
            // date, startDateTime, and endDateTime as these conflict with record override.
            String redriveTag;
            if (tag.startsWith(REDRIVE_TAG_PREFIX)) {
                redriveTag = tag;
            } else {
                redriveTag = REDRIVE_TAG_PREFIX + tag;
            }
            BridgeExporterRequest redriveRequest = new BridgeExporterRequest.Builder().copyOf(request).withDate(null)
                    .withStartDateTime(null).withEndDateTime(null).withRecordIdS3Override(filename).withTag(redriveTag)
                    .withRedriveCount(redriveCount + 1).build();
            LOG.info("Redriving records using S3 file " + filename);

            try {
                // upload to S3
                s3Helper.writeLinesToS3(recordIdOverrideBucket, filename, redriveRecordIdSet);

                // send request to SQS
                sqsHelper.sendMessageAsJson(sqsQueueUrl, redriveRequest, REDRIVE_DELAY_SECONDS);
            } catch (AmazonClientException | IOException ex) {
                // log error, but move on
                LOG.error("Error redriving records: " + ex.getMessage(), ex);
            }
        }

        LOG.info("All subtasks done for request " + request.toString());

        // Tell each health data handler to upload their TSVs to Synapse.
        Set<UploadSchemaKey> redriveTableWhitelist = new HashSet<>();
        for (Map.Entry<UploadSchemaKey, HealthDataExportHandler> healthDataHandlerEntry
                : healthDataHandlersBySchema.entrySet()) {
            UploadSchemaKey schemaKey = healthDataHandlerEntry.getKey();
            HealthDataExportHandler handler = healthDataHandlerEntry.getValue();
            try {
                handler.uploadToSynapseForTask(task);
            } catch (BridgeExporterException | IOException | RuntimeException | SynapseException ex) {
                Throwable originalEx = ex;
                if (originalEx instanceof BridgeExporterTsvException) {
                    // TSV exception is just a wrapper. Go down one level to get the real exception.
                    originalEx = originalEx.getCause();
                }

                if (isSynapseDown(originalEx)) {
                    // Similarly, if Synapse is down, restart BridgeEX.
                    throw new RestartBridgeExporterException("Restarting Bridge Exporter; last schema=" + schemaKey +
                            ": " + originalEx.getMessage(), originalEx);
                } else {
                    LOG.error("Error uploading health data to Synapse for schema=" + schemaKey + ": " +
                            originalEx.getMessage(), originalEx);
                    if (isRetryable(originalEx)) {
                        // Similarly, track which tables (schemas) to redrive.
                        redriveTableWhitelist.add(schemaKey);
                    }
                }
            }
        }
        if (!redriveTableWhitelist.isEmpty() && redriveCount < redriveMaxCount) {
            // Create a copy of the original request, except add the table whitelist and update the tag. This will be
            // used to trigger the redrive.
            String redriveTag;
            if (tag.startsWith(REDRIVE_TAG_PREFIX)) {
                redriveTag = tag;
            } else {
                redriveTag = REDRIVE_TAG_PREFIX + tag;
            }
            BridgeExporterRequest redriveRequest = new BridgeExporterRequest.Builder().copyOf(request)
                    .withTableWhitelist(redriveTableWhitelist).withRedriveCount(redriveCount + 1).withTag(redriveTag)
                    .withIgnoreLastExportTime(true) // only redrive table needs to ignore lastExportDateTime
                    .build();
            LOG.info("Redriving tables: " + BridgeExporterUtil.COMMA_SPACE_JOINER.join(redriveTableWhitelist));

            try {
                sqsHelper.sendMessageAsJson(sqsQueueUrl, redriveRequest, REDRIVE_DELAY_SECONDS);
            } catch (AmazonClientException | JsonProcessingException ex) {
                // log error, but move on
                LOG.error("Error redriving tables: " + ex.getMessage(), ex);
            }
        }

        // Also, the app version handlers.
        for (Map.Entry<String, AppVersionExportHandler> appVersionHandlerEntry
                : appVersionHandlersByStudy.entrySet()) {
            String studyId = appVersionHandlerEntry.getKey();
            AppVersionExportHandler handler = appVersionHandlerEntry.getValue();
            try {
                handler.uploadToSynapseForTask(task);
            } catch (BridgeExporterException | IOException | RuntimeException | SynapseException ex) {
                // TODO: Improved error handling
                // If uploading Bridge data succeeds and somehow the appVersion (index) table fails (including
                // retries), we don't really have a mechanism for redriving this specific table update. Fortunately,
                // the appVersion table is used for diagnostics and is not critical (at the moment), so we can work
                // around it. However, we should think about how to improve this.
                LOG.error("Error uploading app version table to Synapse for study=" + studyId + ": " + ex.getMessage(),
                        ex);
            }
        }

        // Write status table. Status tables are individual for each study.
        for (String oneStudyId : task.getStudyIdSet()) {
            try {
                synapseStatusTableHelper.initTableAndWriteStatus(task, oneStudyId);
            } catch (BridgeExporterException | InterruptedException | RuntimeException | SynapseException ex) {
                // TODO: Improved error handling
                // Similarly, status table is also not critical, but we should think about how to improve this.
                LOG.error("Error writing to status table for study=" + oneStudyId + ": " + ex.getMessage(), ex);
            }
        }

        LOG.info("Done uploading to Synapse for request " + request.toString());
    }

    // Advice from Synapse team is that 503 means Synapse is down (either for maintenance or otherwise). In this case,
    // instead of continuing, we should abort the request and restart BridgeEX immediately.
    //
    // Package-scoped for unit tests.
    static boolean isSynapseDown(Throwable t) {
        return (t instanceof SynapseServerException && ((SynapseServerException)t).getStatusCode() == 503);
    }

    // For redrives, we need to know whether an exception is retryable or not. If it is, we can redrive it. If not, we
    // log the exception but otherwise swallow it.
    //
    // Package-scoped for unit tests.
    static boolean isRetryable(Throwable t) {
        if (t == null) {
            // This should never happen. But if it does, assume something really bad happened and don't try to redrive.
            return false;
        } else if (t instanceof BridgeSDKException) {
            // If this is a Bridge exception in the 400s, this is not retryable.
            int statusCode = ((BridgeSDKException) t).getStatusCode();
            if (statusCode >= 400 && statusCode < 500) {
                return false;
            }
        } else if (t instanceof JsonProcessingException || t instanceof JsonParseException ||
                t instanceof MalformedJsonException) {
            // JSON parse exceptions are generally also not retryable. Note that BridgeEX uses Jackson, but
            // BridgeJavaSDK uses GSON, so we need to handle both.
            // Jackson uses JsonProcessingException as their base exception.
            // GSON uses JsonParseException and MalformedJsonException.
            return false;
        } else if (t instanceof BridgeExporterNonRetryableException) {
            // BridgeExporterNonRetryableExceptions are not retryable. (It's in the name.)
            return false;
        }

        // Everything else should be retryable.
        return true;
    }
}
