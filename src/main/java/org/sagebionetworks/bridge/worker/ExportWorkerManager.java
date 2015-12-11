package org.sagebionetworks.bridge.worker;

import java.util.ArrayList;
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

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;

import org.sagebionetworks.bridge.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.exporter.BridgeExporterConfig;
import org.sagebionetworks.bridge.exporter.ExportHelper;
import org.sagebionetworks.bridge.exporter.UploadSchemaHelper;
import org.sagebionetworks.bridge.exporter.UploadSchemaKey;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.util.BridgeExporterUtil;

public class ExportWorkerManager {
    // Script should report progress after this many tasks remaining, so users tailing the logs can see that it's still
    // making progress.
    private static final int TASK_REMAINING_REPORT_PERIOD = 250;

    // Configured externally
    private BridgeExporterConfig bridgeExporterConfig;
    private DynamoDB ddbClient;
    private ExportHelper exportHelper;
    private S3Helper s3Helper;
    private UploadSchemaHelper schemaHelper;
    private Set<UploadSchemaKey> schemaBlacklist;
    private Set<UploadSchemaKey> schemaWhitelist;
    private SynapseHelper synapseHelper;
    private String todaysDateString;

    // Internal state
    private final List<ExportHandler> allHandlerList = new ArrayList<>();
    private final Map<String, AppVersionExportHandler> appVersionHandlerMap = new HashMap<>();
    private final Map<UploadSchemaKey, HealthDataExportHandler> healthDataHandlerMap = new HashMap<>();
    private final Queue<Future<?>> outstandingTaskQueue = new LinkedList<>();
    private final Set<String> schemasNotFound = new HashSet<>();
    private final Map<String, IosSurveyExportHandler> surveyHandlerMap = new HashMap<>();

    private ExecutorService executor;

    /** Bridge exporter config. Configured externally. */
    public BridgeExporterConfig getBridgeExporterConfig() {
        return bridgeExporterConfig;
    }

    public void setBridgeExporterConfig(BridgeExporterConfig bridgeExporterConfig) {
        this.bridgeExporterConfig = bridgeExporterConfig;
    }

    /** DynamoDB client. Configured externally. */
    public DynamoDB getDdbClient() {
        return ddbClient;
    }

    public void setDdbClient(DynamoDB ddbClient) {
        this.ddbClient = ddbClient;
    }

    /** Export Helper. Configured externally. */
    public ExportHelper getExportHelper() {
        return exportHelper;
    }

    public void setExportHelper(ExportHelper exportHelper) {
        this.exportHelper = exportHelper;
    }

    /** S3 helper. Configured externally. */
    public S3Helper getS3Helper() {
        return s3Helper;
    }

    public void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    /** Schema helper. Configured externally. */
    public UploadSchemaHelper getSchemaHelper() {
        return schemaHelper;
    }

    public void setSchemaHelper(UploadSchemaHelper schemaHelper) {
        this.schemaHelper = schemaHelper;
    }

    /**
     * Blacklist of schema keys to never process. This is generally used for poorly performing or buggy schemas that we
     * need to backfill later. Blacklist takes precedence over whitelist. May be null but will never be empty.
     */
    public Set<UploadSchemaKey> getSchemaBlacklist() {
        return schemaBlacklist;
    }

    public void setSchemaBlacklist(Set<UploadSchemaKey> schemaBlacklist) {
        if (schemaBlacklist != null && !schemaBlacklist.isEmpty()) {
            this.schemaBlacklist = ImmutableSet.copyOf(schemaBlacklist);
        } else {
            this.schemaBlacklist = null;
        }
    }

    /**
     * Whitelist of schema keys that should be the only ones to be processed (other than app version and iOS survey).
     * This is generally used for table backfills. Blacklist takes precedence over whitelist.  May be null but will
     * never be empty.
     */
    public Set<UploadSchemaKey> getSchemaWhitelist() {
        return schemaWhitelist;
    }

    public void setSchemaWhitelist(Set<UploadSchemaKey> schemaWhitelist) {
        if (schemaWhitelist != null && !schemaWhitelist.isEmpty()) {
            this.schemaWhitelist = ImmutableSet.copyOf(schemaWhitelist);
        } else {
            this.schemaWhitelist = null;
        }
    }

    /** Synapse helper. Configured externally. */
    public SynapseHelper getSynapseHelper() {
        return synapseHelper;
    }

    public void setSynapseHelper(SynapseHelper synapseHelper) {
        this.synapseHelper = synapseHelper;
    }

    /**
     * Calendar date in YYYY-MM-DD format, representing when the data is uploaded to Synapse. (Because uploads can take
     * some time, this corresponds with when the upload started. Ideally, the uploads shouldn't take long enough to
     * cross over to the next day.
     */
    public String getTodaysDateString() {
        return todaysDateString;
    }

    public void setTodaysDateString(String todaysDateString) {
        this.todaysDateString = todaysDateString;
    }

    /** Returns a set of schemas that were queried but not found. */
    public Set<String> getSchemasNotFound() {
        return ImmutableSet.copyOf(schemasNotFound);
    }

    /** Initializes the handlers and the executor. */
    public void init() {
        Set<String> studyIdSet = bridgeExporterConfig.getStudyIdSet();

        // per-study init
        for (String oneStudyId : studyIdSet) {
            // init survey handlers
            IosSurveyExportHandler oneSurveyHandler = new IosSurveyExportHandler();
            oneSurveyHandler.setManager(this);
            oneSurveyHandler.setName("surveyHandler-" + oneStudyId);
            oneSurveyHandler.setStudyId(oneStudyId);
            surveyHandlerMap.put(oneStudyId, oneSurveyHandler);

            // init app version handler
            AppVersionExportHandler oneAppVersionHandler = new AppVersionExportHandler();
            oneAppVersionHandler.setManager(this);
            oneAppVersionHandler.setName("appVersionHandler-" + oneStudyId);
            oneAppVersionHandler.setStudyId(oneStudyId);
            appVersionHandlerMap.put(oneStudyId, oneAppVersionHandler);
        }

        // init health data handlers for each schema
        Set<UploadSchemaKey> schemaKeySet;
        if (schemaWhitelist != null) {
            schemaKeySet = schemaWhitelist;
        } else {
            schemaKeySet = schemaHelper.getAllSchemaKeys();
        }
        for (UploadSchemaKey oneSchemaKey : schemaKeySet) {
            // check black list
            if (schemaBlacklist != null && schemaBlacklist.contains(oneSchemaKey)) {
                continue;
            }

            // check against study ID list
            String studyId = oneSchemaKey.getStudyId();
            if (!studyIdSet.contains(studyId)) {
                continue;
            }

            // create handler
            HealthDataExportHandler oneHealthDataHandler = new HealthDataExportHandler();
            oneHealthDataHandler.setManager(this);
            oneHealthDataHandler.setName("healthDataWorker-" + oneSchemaKey.toString());
            oneHealthDataHandler.setSchemaKey(oneSchemaKey);
            oneHealthDataHandler.setStudyId(studyId);
            healthDataHandlerMap.put(oneSchemaKey, oneHealthDataHandler);
        }

        allHandlerList.addAll(surveyHandlerMap.values());
        allHandlerList.addAll(appVersionHandlerMap.values());
        allHandlerList.addAll(healthDataHandlerMap.values());

        // init handlers
        for (ExportHandler oneWorker : allHandlerList) {
            try {
                oneWorker.init();
            } catch (SchemaNotFoundException ex) {
                System.out.println("[ERROR] Schema not found for worker " + oneWorker.getName() + ": "
                        + ex.getMessage());
            } catch (BridgeExporterException ex) {
                System.out.println("[ERROR] Error initializing worker " + oneWorker.getName() + ": "
                        + ex.getMessage());
            } catch (RuntimeException ex) {
                System.out.println("[ERROR] RuntimeException initializing worker for table " + oneWorker.getName()
                        + ": " + ex.getMessage());
                ex.printStackTrace(System.out);
            }
        }

        // executor
        executor = Executors.newFixedThreadPool(bridgeExporterConfig.getNumThreads());
    }

    /**
     * Queues up an iOS survey export task to the manager. This task will also transparently queue up the corresponding
     * task to the right health data export worker and to the study's app version worker.
     */
    public void addIosSurveyExportTask(String studyId, ExportTask task) throws BridgeExporterException {
        // get handler
        IosSurveyExportHandler surveyHandler = surveyHandlerMap.get(studyId);
        if (surveyHandler == null) {
            throw new BridgeExporterException("No survey handler for study " + studyId);
        }

        // create and queue worker
        queueWorker(surveyHandler, task);
    }

    /**
     * Queues up a health data export task to the manager. This also queues up the corresponding app version export
     * task. This is called by the Bridge Exporter as well as the iOS survey export workers.
     */
    public void addHealthDataExportTask(UploadSchemaKey schemaKey, ExportTask task) throws BridgeExporterException,
            SchemaNotFoundException {
        // check black list
        if (schemaBlacklist != null && schemaBlacklist.contains(schemaKey)) {
            return;
        }

        // check white list
        if (schemaWhitelist != null && !schemaWhitelist.contains(schemaKey)) {
            return;
        }

        // check against study ID list
        String studyId = schemaKey.getStudyId();
        if (!bridgeExporterConfig.getStudyIdSet().contains(studyId)) {
            return;
        }

        // get handlers
        HealthDataExportHandler healthDataHandler = healthDataHandlerMap.get(schemaKey);
        if (healthDataHandler == null) {
            schemasNotFound.add(schemaKey.toString());
            throw new SchemaNotFoundException("Schema " + schemaKey.toString() + " not found");
        }
        AppVersionExportHandler appVersionHandler = appVersionHandlerMap.get(studyId);
        if (appVersionHandler == null) {
            throw new BridgeExporterException("No app version worker for study " + studyId);
        }

        // create and queue up workers
        queueWorker(healthDataHandler, task);
        queueWorker(appVersionHandler, task);
    }

    private void queueWorker(ExportHandler handler, ExportTask task) {
        ExportWorker worker = new ExportWorker(handler, task);
        Future<?> future = executor.submit(worker);
        outstandingTaskQueue.add(future);
    }

    /**
     * Signals to the worker manager that the end of the record stream has been reached. This blocks until all workers
     * are finished.
     */
    public void endOfStream() {
        System.out.println("[METRICS] End of stream signaled: " + BridgeExporterUtil.getCurrentLocalTimestamp());

        Stopwatch progressStopwatch = Stopwatch.createStarted();
        while (!outstandingTaskQueue.isEmpty()) {
            int numOutstanding = outstandingTaskQueue.size();
            if (numOutstanding % TASK_REMAINING_REPORT_PERIOD == 0) {
                System.out.println("[METRICS] Num outstanding tasks: " + numOutstanding + " after " +
                        progressStopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
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

        // Once there are no outstanding tasks, we can shutdown all of our handlers. This should be quick (~30 sec).
        for (ExportHandler oneHandler : allHandlerList) {
            oneHandler.endOfStream();
        }

        System.out.println("[METRICS] Done uploading TSVs: " + BridgeExporterUtil.getCurrentLocalTimestamp());

        // Second loop to report metrics, now that all the uploads are done.
        for (ExportHandler oneHandler : allHandlerList) {
            oneHandler.reportMetrics();
        }
    }
}
