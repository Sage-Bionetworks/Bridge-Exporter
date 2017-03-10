package org.sagebionetworks.bridge.exporter.synapse;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.joda.time.LocalDate;
import org.mockito.ArgumentCaptor;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.PartialRow;
import org.sagebionetworks.repo.model.table.PartialRowSet;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exporter.worker.ExportTask;
import org.sagebionetworks.bridge.exporter.worker.ExportWorkerManager;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class SynapseStatusTableHelperTest {
    private static final String COLUMN_ID = "test-column-id";
    private static final long DATA_ACCESS_TEAM_ID = 1234;
    private static final long SYNAPSE_PRINCIPAL_ID = 5678;
    private static final String SYNAPSE_PROJECT_ID = "test-synapse-project";
    private static final String SYNAPSE_TABLE_ID = "test-synapse-table";

    private static final String STUDY_ID = "test-study";
    private static final String SYNAPSE_TABLE_NAME = "test-study-status";

    private String synapseTableId;

    @BeforeMethod
    public void before() {
        // clear helper vars, because TestNG doesn't always do that
        synapseTableId = null;
    }

    @Test
    public void test() throws Exception {
        // mock manager
        ExportWorkerManager mockManager = mock(ExportWorkerManager.class);
        when(mockManager.getDataAccessTeamIdForStudy(STUDY_ID)).thenReturn(DATA_ACCESS_TEAM_ID);
        when(mockManager.getSynapsePrincipalId()).thenReturn(SYNAPSE_PRINCIPAL_ID);
        when(mockManager.getSynapseProjectIdForStudyAndTask(eq(STUDY_ID), notNull(ExportTask.class))).thenReturn(
                SYNAPSE_PROJECT_ID);

        // mock get/setSynapseTableIdFromDdb
        when(mockManager.getSynapseTableIdFromDdb(notNull(ExportTask.class),
                eq(SynapseHelper.DDB_TABLE_SYNAPSE_META_TABLES), eq(SynapseHelper.DDB_KEY_TABLE_NAME),
                eq(SYNAPSE_TABLE_NAME))).thenAnswer(invocation -> synapseTableId);
        doAnswer(invocation -> synapseTableId = invocation.getArgumentAt(4, String.class)).when(mockManager)
                .setSynapseTableIdToDdb(notNull(ExportTask.class), eq(SynapseHelper.DDB_TABLE_SYNAPSE_META_TABLES),
                        eq(SynapseHelper.DDB_KEY_TABLE_NAME), eq(SYNAPSE_TABLE_NAME), anyString());

        // mock Synapse Helper
        SynapseHelper mockSynapseHelper = mock(SynapseHelper.class);
        mockManager.setSynapseHelper(mockSynapseHelper);
        when(mockSynapseHelper.createTableWithColumnsAndAcls(SynapseStatusTableHelper.COLUMN_LIST, DATA_ACCESS_TEAM_ID,
                SYNAPSE_PRINCIPAL_ID, SYNAPSE_PROJECT_ID, SYNAPSE_TABLE_NAME)).thenReturn(SYNAPSE_TABLE_ID);

        // column list from Synapse needs column name (because we check for it) and column ID (because we use it
        ColumnModel serverSideColumn = new ColumnModel();
        serverSideColumn.setName(SynapseStatusTableHelper.COLUMN_NAME_UPLOAD_DATE);
        serverSideColumn.setId(COLUMN_ID);
        when(mockSynapseHelper.getColumnModelsForTableWithRetry(SYNAPSE_TABLE_ID)).thenReturn(ImmutableList.of(
                serverSideColumn));
        when(mockSynapseHelper.getTableWithRetry(SYNAPSE_TABLE_ID)).thenReturn(new TableEntity());

        // set up helper
        SynapseStatusTableHelper statusTableHelper = new SynapseStatusTableHelper();
        statusTableHelper.setManager(mockManager);
        statusTableHelper.setSynapseHelper(mockSynapseHelper);

        // execute
        // initial call creates the table
        ExportTask task1 = mock(ExportTask.class);
        when(task1.getExporterDate()).thenReturn(LocalDate.parse("2016-03-06"));
        statusTableHelper.initTableAndWriteStatus(task1, STUDY_ID);

        // verify we created the table
        verify(mockSynapseHelper).getTableWithRetry(any());
        verify(mockSynapseHelper, times(1)).createTableWithColumnsAndAcls(SynapseStatusTableHelper.COLUMN_LIST,
                DATA_ACCESS_TEAM_ID, SYNAPSE_PRINCIPAL_ID, SYNAPSE_PROJECT_ID, SYNAPSE_TABLE_NAME);
        verify(mockManager, times(1)).setSynapseTableIdToDdb(task1, SynapseHelper.DDB_TABLE_SYNAPSE_META_TABLES,
                SynapseHelper.DDB_KEY_TABLE_NAME, SYNAPSE_TABLE_NAME, SYNAPSE_TABLE_ID);

        // verify write to Synapse
        ArgumentCaptor<PartialRowSet> rowSetCaptor1 = ArgumentCaptor.forClass(PartialRowSet.class);
        verify(mockSynapseHelper, times(1)).appendRowsToTableWithRetry(rowSetCaptor1.capture(), eq(SYNAPSE_TABLE_ID));

        PartialRowSet rowSet1 = rowSetCaptor1.getValue();
        assertEquals(rowSet1.getTableId(), SYNAPSE_TABLE_ID);

        List<PartialRow> rowList1 = rowSet1.getRows();
        assertEquals(rowList1.size(), 1);

        PartialRow row1 = rowList1.get(0);
        Map<String, String> rowValueMap1 = row1.getValues();
        assertEquals(rowValueMap1.size(), 1);
        assertEquals(rowValueMap1.get(COLUMN_ID), "2016-03-06");

        // second call, tables already exist
        ExportTask task2 = mock(ExportTask.class);
        when(task2.getExporterDate()).thenReturn(LocalDate.parse("2016-03-07"));
        statusTableHelper.initTableAndWriteStatus(task2, STUDY_ID);

        // Verify we only tried to create the table once. (verify() is cumulative, so times(1) means we verified it the
        // first time around, and it didn't happen again.)
        verify(mockSynapseHelper, times(1)).createTableWithColumnsAndAcls(anyListOf(ColumnModel.class), anyLong(),
                anyLong(), anyString(), anyString());
        verify(mockManager, times(1)).setSynapseTableIdToDdb(any(ExportTask.class), anyString(), anyString(),
                anyString(), anyString());

        // Verify a second write to Synapse. (Again, verify() is cumulative.)
        ArgumentCaptor<PartialRowSet> rowSetCaptor2 = ArgumentCaptor.forClass(PartialRowSet.class);
        verify(mockSynapseHelper, times(2)).appendRowsToTableWithRetry(rowSetCaptor2.capture(), eq(SYNAPSE_TABLE_ID));

        PartialRowSet rowSet2 = rowSetCaptor2.getValue();
        assertEquals(rowSet2.getTableId(), SYNAPSE_TABLE_ID);

        List<PartialRow> rowList2 = rowSet2.getRows();
        assertEquals(rowList2.size(), 1);

        PartialRow row2 = rowList2.get(0);
        Map<String, String> rowValueMap2 = row2.getValues();
        assertEquals(rowValueMap2.size(), 1);
        assertEquals(rowValueMap2.get(COLUMN_ID), "2016-03-07");
    }
}
