package org.sagebionetworks.bridge.exporter.record;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Resource;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.base.Stopwatch;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.schema.adapter.JSONObjectAdapterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.exceptions.RestartBridgeExporterException;
import org.sagebionetworks.bridge.exporter.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.exporter.exceptions.SynapseUnavailableException;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.metrics.MetricsHelper;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.synapse.SynapseHelper;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.exporter.worker.ExportTask;
import org.sagebionetworks.bridge.exporter.worker.ExportWorkerManager;
import org.sagebionetworks.bridge.file.FileHelper;

/**
 * This is the main entry point into Bridge EX. This record processor class is called for each request, and loops over
 * all health data records in that request. For each health data record, this does basic processing and filtering, and
 * identifies the correct schema for the record before handing it off to the {@link ExportWorkerManager}.
 */
@Component
public class BridgeExporterRecordProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeExporterRecordProcessor.class);

    // package-scoped to be available to unit tests
    static final String CONFIG_KEY_RECORD_LOOP_DELAY_MILLIS = "record.loop.delay.millis";
    static final String CONFIG_KEY_RECORD_LOOP_PROGRESS_REPORT_PERIOD = "record.loop.progress.report.period";
    static final String STUDY_ID = "studyId";
    static final String LAST_EXPORT_DATE_TIME = "lastExportDateTime";

    // config attributes
    private int delayMillis;
    private int progressReportPeriod;
    private DateTimeZone timeZone;

    // Spring helpers
    private Table ddbRecordTable;
    private Table ddbExportTimeTable;
    private FileHelper fileHelper;
    private MetricsHelper metricsHelper;
    private RecordFilterHelper recordFilterHelper;
    private RecordIdSourceFactory recordIdSourceFactory;
    private SynapseHelper synapseHelper;
    private ExportWorkerManager workerManager;

    /** Config, used to get attributes for loop control and time zone. */
    @Autowired
    public final void setConfig(Config config) {
        this.delayMillis = config.getInt(CONFIG_KEY_RECORD_LOOP_DELAY_MILLIS);
        this.progressReportPeriod = config.getInt(CONFIG_KEY_RECORD_LOOP_PROGRESS_REPORT_PERIOD);
        this.timeZone = DateTimeZone.forID(config.get(BridgeExporterUtil.CONFIG_KEY_TIME_ZONE_NAME));
    }

    /** DDB Health Data Record table, used for querying full records for the list of record IDs. */
    @Resource(name = "ddbRecordTable")
    public final void setDdbRecordTable(Table ddbRecordTable) {
        this.ddbRecordTable = ddbRecordTable;
    }

    /** DDB Export Time Table. */
    @Resource(name = "ddbExportTimeTable")
    final void setDdbExportTimeTable(Table ddbExportTimeTable) {
        this.ddbExportTimeTable = ddbExportTimeTable;
    }

    /** File helper, used for creating and cleaning up the temp dir used to store the request's temporary files. */
    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    /** Metrics helper, used for basic metrics operations. */
    @Autowired
    public final void setMetricsHelper(MetricsHelper metricsHelper) {
        this.metricsHelper = metricsHelper;
    }

    /**
     * Record filter helper, used to determine which records to filter out, due to request filters or sharing filters.
     */
    @Autowired
    public final void setRecordFilterHelper(RecordFilterHelper recordFilterHelper) {
        this.recordFilterHelper = recordFilterHelper;
    }

    /** Record ID source factor. Given the request, this generates the list of record IDs to be exported. */
    @Autowired
    public final void setRecordIdSourceFactory(RecordIdSourceFactory recordIdSourceFactory) {
        this.recordIdSourceFactory = recordIdSourceFactory;
    }

    /** Synapse Helper, used to check Synapse health status before starting export job. */
    @Autowired
    public final void setSynapseHelper(SynapseHelper synapseHelper) {
        this.synapseHelper = synapseHelper;
    }

    /**
     * Worker manager, which takes export tasks and subtasks generated by this class and manages the workers and
     * handlers to perform those tasks, and eventually uploads the export results to Synapse.
     */
    @Autowired
    public final void setWorkerManager(ExportWorkerManager workerManager) {
        this.workerManager = workerManager;
    }

    /**
     * Main entry point into Bridge-EX. This process all records for the given request.
     *
     * @param request
     *         request to process records for
     * @throws IOException
     *         if we fail to get the record IDs from the record ID factory
     * @throws SynapseUnavailableException
     *         if Synapse is not available in read/write mode
     */
    public void processRecordsForRequest(BridgeExporterRequest request) throws IOException,
            RestartBridgeExporterException, SynapseUnavailableException {
        LOG.info("Received request " + request.toString());

        // Check to see that Synapse is up and availabe for read/write. If it isn't, throw an exception, so the
        // PollSqsWorker can re-cycle the request until Synapse is available again.
        boolean isSynapseWritable;
        try {
            isSynapseWritable = synapseHelper.isSynapseWritable();
        } catch (JSONObjectAdapterException | SynapseException ex) {
            throw new SynapseUnavailableException("Error calling Synapse: " + ex.getMessage(), ex);
        }
        if (!isSynapseWritable) {
            throw new SynapseUnavailableException("Synapse not in writable state");
        }

        // make task
        Metrics metrics = new Metrics();
        File tmpDir = fileHelper.createTempDir();
        ExportTask task = new ExportTask.Builder().withExporterDate(LocalDate.now(timeZone)).withMetrics(metrics)
                .withRequest(request).withTmpDir(tmpDir).build();

        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            RecordAndStudyListHolder recordAndStudyListHolder = recordIdSourceFactory.getRecordSourceForRequest(request);
            Iterable<String> recordIdIterable = recordAndStudyListHolder.getRecordIdSource();
            Map<String, DateTime> studyIdsToUpdate = recordAndStudyListHolder.getStudyIdsToQuery();
            long endDateTimeEpoch = recordAndStudyListHolder.getEndDateTime().getMillis();

            for (String oneRecordId : recordIdIterable) {
                // Count total number of records. Also, log at regular intervals, so people tailing the logs can follow
                // progress.
                int numTotal = metrics.incrementCounter("numTotal");
                if (numTotal % progressReportPeriod == 0) {
                    LOG.info("Num records so far: " + numTotal + " in " + stopwatch.elapsed(TimeUnit.SECONDS) +
                            " seconds");
                }

                // sleep to rate limit our requests to DDB
                if (delayMillis > 0) {
                    try {
                        Thread.sleep(delayMillis);
                    } catch (InterruptedException ex) {
                        LOG.error("Record processor interrupted while sleeping: " + ex.getMessage(), ex);
                    }
                }

                try {
                    // get record
                    Item record = ddbRecordTable.getItem("id", oneRecordId);
                    if (record == null) {
                        LOG.error("Missing health data record for ID " + oneRecordId);
                        continue;
                    }

                    // filter
                    boolean shouldExcludeRecord = recordFilterHelper.shouldExcludeRecord(metrics, request, record);
                    if (shouldExcludeRecord) {
                        continue;
                    }

                    // only after the filter do we log health code metrics
                    metricsHelper.captureMetricsForRecord(metrics, record);

                    workerManager.addSubtaskForRecord(task, record);
                } catch (IOException | RuntimeException | SchemaNotFoundException ex) {
                    LOG.error("Exception processing record " + oneRecordId + ": " + ex.getMessage(), ex);
                }
            }

            workerManager.endOfStream(task);

            // We made it to the end. Set the success flag on the task.
            setTaskSuccess(task);

            // update ddb exportTimeTable
            if (!studyIdsToUpdate.isEmpty()) {
                for (String studyId: studyIdsToUpdate.keySet()) {
                    ddbExportTimeTable.putItem(new Item().withPrimaryKey(STUDY_ID, studyId).withNumber(LAST_EXPORT_DATE_TIME, endDateTimeEpoch));
                }
            }
        } finally {
            long elapsedTime = stopwatch.elapsed(TimeUnit.SECONDS);
            if (task.isSuccess()) {
                LOG.info("Finished processing request in " + elapsedTime + " seconds, " + request.toString());
            } else {
                LOG.error("Error processing request; elapsed time " + elapsedTime + " seconds, " + request.toString());
            }
            metricsHelper.publishMetrics(metrics);
        }

        // cleanup
        fileHelper.deleteDir(tmpDir);
    }

    // Helper method that we can spy and verify that we're setting the task success properly.
    void setTaskSuccess(ExportTask task) {
        task.setSuccess(true);
    }
}
