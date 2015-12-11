package org.sagebionetworks.bridge.worker;

import org.sagebionetworks.bridge.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exceptions.SchemaNotFoundException;

/**
 * Generic export handler. This class encapsulates basic configuration, helpers, and clients needed by export workers
 * and serves as the parent class for both Synapse export workers and iOS survey export workers.
 */
public abstract class ExportHandler {
    // Configured externally
    private ExportWorkerManager manager;
    private String name;
    private String studyId;

    /**
     * Returns team ID of the team allowed to read the data. This is derived from the study and from the study to
     * project mapping in the Bridge Exporter config.
     */
    public Long getDataAccessTeamId() {
        return manager.getBridgeExporterConfig().getDataAccessTeamIdsByStudy().get(studyId);
    }

    /** Export worker manager. Configured externally. */
    public ExportWorkerManager getManager() {
        return manager;
    }

    public void setManager(ExportWorkerManager manager) {
        this.manager = manager;
    }

    /**
     * Returns the Synapse project ID. This is derived from the study and from the study to project mapping in the
     * Bridge Exporter config.
     */
    public String getProjectId() {
        return manager.getBridgeExporterConfig().getProjectIdsByStudy().get(studyId);
    }

    /** Handler name. */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /** Study ID. Configured externally. */
    public String getStudyId() {
        return studyId;
    }

    public void setStudyId(String studyId) {
        this.studyId = studyId;
    }

    /** Initializes the worker. This is called by the ExportWorkerManager before starting the worker. */
    public abstract void init() throws BridgeExporterException, SchemaNotFoundException;

    /** Handles the task. Whatever it might be. */
    public abstract void handle(ExportTask task);

    /**
     * Signals the end of stream to the handler. This tells the handler to do things like upload the TSV to Synapse.
     */
    public abstract void endOfStream();

    /**
     * Reports the metrics the worker has collected, if any. This is called by the ExportWorkerManager upon reaching
     * end of stream.
     */
    public abstract void reportMetrics();
}
