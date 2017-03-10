package org.sagebionetworks.bridge.exporter.handler;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.sagebionetworks.client.exceptions.SynapseNotFoundException;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.exporter.helper.BridgeHelperTest;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.synapse.ColumnDefinition;
import org.sagebionetworks.bridge.exporter.synapse.SynapseHelper;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.exporter.util.TestUtil;
import org.sagebionetworks.bridge.exporter.worker.ExportSubtask;
import org.sagebionetworks.bridge.exporter.worker.ExportTask;
import org.sagebionetworks.bridge.exporter.worker.ExportWorkerManager;
import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.rest.model.UploadFieldDefinition;
import org.sagebionetworks.bridge.rest.model.UploadFieldType;
import org.sagebionetworks.bridge.rest.model.UploadSchema;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

public class SynapseExportHandlerNewTableTest {
    private static final List<ColumnModel> MOCK_COLUMN_LIST;

    private static final List<ColumnDefinition> MOCK_COLUMN_DEFINITION;

    static {
        MOCK_COLUMN_DEFINITION = SynapseExportHandlerTest.createTestSynapseColumnDefinitions();
        MOCK_COLUMN_LIST = SynapseExportHandlerTest.createTestSynapseColumnList(MOCK_COLUMN_DEFINITION);
    }

    private String ddbSynapseTableId;
    private ExportWorkerManager manager;
    private SynapseHelper mockSynapseHelper;
    private ExportTask task;
    private byte[] tsvBytes;

    @BeforeMethod
    public void before() {
        // clear test vars, because TestNG doesn't always do that
        ddbSynapseTableId = null;
        tsvBytes = null;
    }

    private void setup(SynapseExportHandler handler) throws Exception {
        setupWithSchema(handler, null, null);
    }

    private void setupWithSchema(SynapseExportHandler handler, UploadSchemaKey schemaKey, UploadSchema schema)
            throws Exception {
        // This needs to be done first, because lots of stuff reference this, even while we're setting up mocks.
        handler.setStudyId(SynapseExportHandlerTest.TEST_STUDY_ID);

        // mock BridgeHelper
        BridgeHelper mockBridgeHelper = mock(BridgeHelper.class);
        when(mockBridgeHelper.getSchema(any(), eq(schemaKey))).thenReturn(schema);

        // mock config
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(ExportWorkerManager.CONFIG_KEY_EXPORTER_DDB_PREFIX)).thenReturn(
                SynapseExportHandlerTest.DUMMY_DDB_PREFIX);
        when(mockConfig.getInt(ExportWorkerManager.CONFIG_KEY_SYNAPSE_PRINCIPAL_ID))
                .thenReturn(SynapseExportHandlerTest.TEST_SYNAPSE_PRINCIPAL_ID);
        when(mockConfig.getInt(ExportWorkerManager.CONFIG_KEY_WORKER_MANAGER_PROGRESS_REPORT_PERIOD)).thenReturn(250);

        // mock file helper
        InMemoryFileHelper mockFileHelper = new InMemoryFileHelper();
        File tmpDir = mockFileHelper.createTempDir();

        // Mock Synapse Helper - We'll fill in the behavior later, because due to the way this test is constructed, we
        // need to set up the Manager before we can properly mock the Synapse Helper.
        mockSynapseHelper = mock(SynapseHelper.class);

        // setup manager - This is only used to get helper objects.
        manager = spy(new ExportWorkerManager());
        manager.setBridgeHelper(mockBridgeHelper);
        manager.setConfig(mockConfig);
        manager.setFileHelper(mockFileHelper);
        manager.setSynapseHelper(mockSynapseHelper);
        manager.setSynapseColumnDefinitions(MOCK_COLUMN_DEFINITION);
        handler.setManager(manager);

        // set up task
        task = new ExportTask.Builder().withExporterDate(SynapseExportHandlerTest.DUMMY_REQUEST_DATE)
                .withMetrics(new Metrics()).withRequest(SynapseExportHandlerTest.DUMMY_REQUEST).withTmpDir(tmpDir)
                .build();

        // mock create columns - all we care about are column names and IDs
        List<ColumnModel> columnModelList = new ArrayList<>();
        columnModelList.addAll(MOCK_COLUMN_LIST);
        columnModelList.addAll(handler.getSynapseTableColumnList(task));

        // mock create table with columns and ACLs
        when(mockSynapseHelper.createTableWithColumnsAndAcls(columnModelList,
                SynapseExportHandlerTest.TEST_SYNAPSE_DATA_ACCESS_TEAM_ID,
                SynapseExportHandlerTest.TEST_SYNAPSE_PRINCIPAL_ID, SynapseExportHandlerTest.TEST_SYNAPSE_PROJECT_ID,
                handler.getDdbTableKeyValue())).thenReturn(SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID);

        // mock get column model list
        when(mockSynapseHelper.getColumnModelsForTableWithRetry(SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID))
                .thenReturn(columnModelList);

        // mock upload the TSV and capture the upload
        when(mockSynapseHelper.uploadTsvFileToTable(eq(SynapseExportHandlerTest.TEST_SYNAPSE_PROJECT_ID),
                eq(SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID), notNull(File.class))).thenAnswer(invocation -> {
            // on cleanup, the file is destroyed, so we need to intercept that file now
            File tsvFile = invocation.getArgumentAt(2, File.class);
            tsvBytes = mockFileHelper.getBytes(tsvFile);

            // we processed 1 rows
            return 1;
        });

        // spy getSynapseProjectId and getDataAccessTeam
        // These calls through to a bunch of stuff (which we test in ExportWorkerManagerTest), so to simplify our test,
        // we just use a spy here.
        doReturn(SynapseExportHandlerTest.TEST_SYNAPSE_PROJECT_ID).when(manager).getSynapseProjectIdForStudyAndTask(
                eq(SynapseExportHandlerTest.TEST_STUDY_ID), same(task));
        doReturn(SynapseExportHandlerTest.TEST_SYNAPSE_DATA_ACCESS_TEAM_ID).when(manager).getDataAccessTeamIdForStudy(
                SynapseExportHandlerTest.TEST_STUDY_ID);

        // Similarly, spy get/setSynapseTableIdFromDDB.
        doAnswer(invocation -> ddbSynapseTableId).when(manager).getSynapseTableIdFromDdb(task,
                handler.getDdbTableName(), handler.getDdbTableKeyName(), handler.getDdbTableKeyValue());
        doAnswer(invocation -> ddbSynapseTableId = invocation.getArgumentAt(4, String.class)).when(manager)
                .setSynapseTableIdToDdb(same(task), eq(handler.getDdbTableName()), eq(handler.getDdbTableKeyName()),
                        eq(handler.getDdbTableKeyValue()), anyString());
    }

    private void validateTableCreation(SynapseExportHandler handler) throws Exception {
        // Don't bother validating metrics or line counts or even file cleanup. This is all tested in the normal case.
        // Just worry about Synapse table creation.

        // validate setSynapseTableIdToDdb
        verify(manager).setSynapseTableIdToDdb(task, handler.getDdbTableName(), handler.getDdbTableKeyName(),
                handler.getDdbTableKeyValue(), SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID);
    }

    @Test
    public void genericHandlerTest() throws Exception {
        SynapseExportHandler handler = new TestSynapseHandler();
        setup(handler);

        // execute
        handler.handle(SynapseExportHandlerTest.makeSubtask(task, "foo", "single record"));
        handler.uploadToSynapseForTask(task);

        // validate tsv file
        List<String> tsvLineList = TestUtil.bytesToLines(tsvBytes);
        assertEquals(tsvLineList.size(), 2);
        SynapseExportHandlerTest.validateTsvHeaders(tsvLineList.get(0), "foo");
        SynapseExportHandlerTest.validateTsvRow(tsvLineList.get(1), "single record");

        validateTableCreation(handler);
    }

    @Test
    public void appVersionExportHandlerTest() throws Exception {
        SynapseExportHandler handler = new AppVersionExportHandler();
        setup(handler);

        // execute
        handler.handle(SynapseExportHandlerTest.makeSubtask(task, "{}"));
        handler.uploadToSynapseForTask(task);

        // validate tsv file
        List<String> tsvLineList = TestUtil.bytesToLines(tsvBytes);
        assertEquals(tsvLineList.size(), 2);
        SynapseExportHandlerTest.validateTsvHeaders(tsvLineList.get(0), "originalTable");
        SynapseExportHandlerTest.validateTsvRow(tsvLineList.get(1),
                SynapseExportHandlerTest.DUMMY_SCHEMA_KEY.toString());

        validateTableCreation(handler);
    }

    @Test
    public void appVersionExportHandlerTestWithValidTableIdButNoTable() throws Exception {
        SynapseExportHandler handler = new AppVersionExportHandler();
        setup(handler);

        // change some stub logic
        this.ddbSynapseTableId = SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID;
        when(mockSynapseHelper.getTableWithRetry(SynapseExportHandlerTest.TEST_SYNAPSE_TABLE_ID)).thenThrow(
                new SynapseNotFoundException());

        // execute
        handler.handle(SynapseExportHandlerTest.makeSubtask(task, "{}"));
        handler.uploadToSynapseForTask(task);

        // validate tsv file
        verify(mockSynapseHelper).getTableWithRetry(any());
        List<String> tsvLineList = TestUtil.bytesToLines(tsvBytes);
        assertEquals(tsvLineList.size(), 2);
        SynapseExportHandlerTest.validateTsvHeaders(tsvLineList.get(0), "originalTable");
        SynapseExportHandlerTest.validateTsvRow(tsvLineList.get(1),
                SynapseExportHandlerTest.DUMMY_SCHEMA_KEY.toString());

        validateTableCreation(handler);
    }

    @Test
    public void healthDataExportHandlerTest() throws Exception {
        // Freeform text to attachments is already tested in SynapseExportHandlerTest. Just test simple string and int
        // fields.
        UploadSchema testSchema = BridgeHelperTest.simpleSchemaBuilder().fieldDefinitions(ImmutableList.of(
                new UploadFieldDefinition().name("foo").type(UploadFieldType.STRING),
                new UploadFieldDefinition().name("bar").type(UploadFieldType.INT)));
        UploadSchemaKey testSchemaKey = BridgeExporterUtil.getSchemaKeyFromSchema(testSchema);

        // Set up handler and test. setSchema() needs to be called before setup, since a lot of the stuff in the
        // handler depends on it, even while we're mocking stuff.
        HealthDataExportHandler handler = new HealthDataExportHandler();
        handler.setSchemaKey(testSchemaKey);
        setupWithSchema(handler, testSchemaKey, testSchema);

        // mock serializeToSynapseType() - We actually call through to the real method. Don't need to mock
        // uploadFromS3ToSynapseFileHandle() because we don't have file handles this time.
        when(mockSynapseHelper.serializeToSynapseType(any(), any(), any(), any(), any(), any())).thenCallRealMethod();

        // make subtask
        String recordJsonText = "{\n" +
                "   \"foo\":\"This is a string.\",\n" +
                "   \"bar\":42\n" +
                "}";
        ExportSubtask subtask = SynapseExportHandlerTest.makeSubtask(task, recordJsonText);

        // execute
        handler.handle(subtask);
        handler.uploadToSynapseForTask(task);

        // validate tsv file
        List<String> tsvLineList = TestUtil.bytesToLines(tsvBytes);
        assertEquals(tsvLineList.size(), 2);
        SynapseExportHandlerTest.validateTsvHeaders(tsvLineList.get(0), "foo", "bar");
        SynapseExportHandlerTest.validateTsvRow(tsvLineList.get(1), "This is a string.", "42");

        validateTableCreation(handler);
    }
}
