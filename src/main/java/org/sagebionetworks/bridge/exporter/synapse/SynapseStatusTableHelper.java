package org.sagebionetworks.bridge.exporter.synapse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exporter.worker.ExportTask;
import org.sagebionetworks.bridge.exporter.worker.ExportWorkerManager;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseNotFoundException;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.sagebionetworks.repo.model.table.PartialRow;
import org.sagebionetworks.repo.model.table.PartialRowSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Highly specialized helper, which writes the status row to the Synapse status table, creating it if it doesn't
 * already exist.
 */
@Component
public class SynapseStatusTableHelper {
    // Constants package-scoped to be available to unit tests.
    static final String COLUMN_NAME_UPLOAD_DATE = "uploadDate";
    static final List<ColumnModel> COLUMN_LIST;
    static {
        // Construct column definition list. This table is only used as a flag to signal that we're done uploading, so
        // we only need one column with the date.
        // NOTE: ColumnType.DATE is actually a timestamp. There is no calendar date type.
        ColumnModel uploadDateColumn = new ColumnModel();
        uploadDateColumn.setName(COLUMN_NAME_UPLOAD_DATE);
        uploadDateColumn.setColumnType(ColumnType.STRING);
        uploadDateColumn.setMaximumSize(10L);

        COLUMN_LIST = ImmutableList.of(uploadDateColumn);
    }

    private ExportWorkerManager manager;
    private SynapseHelper synapseHelper;

    /**
     * Export worker manager, used for its many utility methods, like get/setSynapseTableIdFromDdb, data access team,
     * principal ID, parent project ID, etc.
     */
    @Autowired
    final void setManager(ExportWorkerManager manager) {
        this.manager = manager;
    }

    /** Synapse Helper, used to create the Synapse table. */
    @Autowired
    final void setSynapseHelper(SynapseHelper synapseHelper) {
        this.synapseHelper = synapseHelper;
    }

    /**
     * Writes the export task status to the status table for the given export task and study. If the status table
     * doesn't exist, this will create it.
     *
     * @param task
     *         export task to write status for
     * @param studyId
     *         study ID to write status for
     * @throws BridgeExporterException
     *         if there's an unusual condition, like mismatched table columns
     * @throws InterruptedException
     *         if a Synapse asynchronous request thread is interrupted
     * @throws SynapseException
     *         if the Synapse call fails
     */
    public void initTableAndWriteStatus(ExportTask task, String studyId) throws BridgeExporterException,
            InterruptedException, SynapseException {
        // Does the table already exist? If not, create it.
        String synapseTableId = manager.getSynapseTableIdFromDdb(task, SynapseHelper.DDB_TABLE_SYNAPSE_META_TABLES,
                SynapseHelper.DDB_KEY_TABLE_NAME, getStatusTableName(studyId));

        // check if the status table exists in synapse
        SynapseHelper synapseHelper = manager.getSynapseHelper();
        boolean isExisted = true;
        try {
            synapseHelper.getTableWithRetry(synapseTableId);
        } catch (SynapseNotFoundException e) {
            isExisted = false;
        }

        if (synapseTableId == null || !isExisted) {
            synapseTableId = createStatusTable(task, studyId);
        }

        // Table definitely exists now. Write status with this internal helper.
        writeStatus(synapseTableId, task, studyId);
    }

    // Helper method that abstracts away the status table name, which is always "[studyId]-status".
    private String getStatusTableName(String studyId) {
        return studyId + "-status";
    }

    // Helper method to create the status table. It sets up the columns, then calls through Synapse Helper.
    private String createStatusTable(ExportTask task, String studyId) throws BridgeExporterException,
            SynapseException {
        // Delegate table creation to SynapseHelper.
        long dataAccessTeamId = manager.getDataAccessTeamIdForStudy(studyId);
        long principalId = manager.getSynapsePrincipalId();
        String projectId = manager.getSynapseProjectIdForStudyAndTask(studyId, task);
        String tableName = getStatusTableName(studyId);
        String synapseTableId = synapseHelper.createTableWithColumnsAndAcls(COLUMN_LIST, dataAccessTeamId, principalId,
                projectId, tableName);

        // write back to DDB table
        manager.setSynapseTableIdToDdb(task, SynapseHelper.DDB_TABLE_SYNAPSE_META_TABLES,
                SynapseHelper.DDB_KEY_TABLE_NAME, getStatusTableName(studyId), synapseTableId);

        return synapseTableId;
    }

    // Helper method to write the actual status. It creates a partial row set, then calls through to the Synapse
    // Helper to write the row set.
    private void writeStatus(String synapseTableId, ExportTask task, String studyId) throws BridgeExporterException,
            InterruptedException, SynapseException {
        // The row set map is one column ID, so we need to get the column model from the table.
        List<ColumnModel> columnList = synapseHelper.getColumnModelsForTableWithRetry(synapseTableId);
        if (columnList.size() != 1) {
            throw new BridgeExporterException("Wrong number of columns in status table for study " + studyId +
                    ", expected 1 column, got " + columnList.size() + " columns");
        }

        // We know there is exactly one column in the status table.
        ColumnModel column = columnList.get(0);
        String colName = column.getName();
        if (!COLUMN_NAME_UPLOAD_DATE.equals(colName)) {
            throw new BridgeExporterException("Wrong column in status table for study " + studyId + ", expected " +
                    COLUMN_NAME_UPLOAD_DATE + ", got " + colName);
        }

        String colId = column.getId();

        // Make row set. We only need to write one row, and that row only has one column: the upload date.
        PartialRow row = new PartialRow();
        row.setValues(ImmutableMap.of(colId, task.getExporterDate().toString()));

        PartialRowSet rowSet = new PartialRowSet();
        rowSet.setRows(ImmutableList.of(row));
        rowSet.setTableId(synapseTableId);

        // write to Synapse
        synapseHelper.appendRowsToTableWithRetry(rowSet, synapseTableId);
    }
}
