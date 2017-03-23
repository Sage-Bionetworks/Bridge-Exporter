package org.sagebionetworks.bridge.exporter.record;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.sagebionetworks.bridge.exporter.record.BridgeExporterRecordProcessor.LAST_EXPORT_DATE_TIME;
import static org.sagebionetworks.bridge.exporter.record.BridgeExporterRecordProcessor.STUDY_ID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.exceptions.RestartBridgeExporterException;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.metrics.MetricsHelper;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.synapse.SynapseHelper;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.exporter.worker.ExportTask;
import org.sagebionetworks.bridge.exporter.worker.ExportWorkerManager;
import org.sagebionetworks.bridge.file.InMemoryFileHelper;

@SuppressWarnings("unchecked")
public class BridgeExporterRecordProcessorTest {
    private static final BridgeExporterRequest REQUEST = new BridgeExporterRequest.Builder()
            .withDate(LocalDate.parse("2015-11-04")).withTag("unit-test-tag").build();
    private static final String END_DATE_TIME_STR = "2016-05-09T23:59:59.999-0700";
    private static final DateTime END_DATE_TIME = DateTime.parse(END_DATE_TIME_STR);

    private Table mockDdbRecordTable;
    private InMemoryFileHelper mockFileHelper;
    private ExportWorkerManager mockManager;
    private MetricsHelper mockMetricsHelper;
    private RecordFilterHelper mockRecordFilterHelper;
    private RecordIdSourceFactory mockRecordIdFactory;
    private BridgeExporterRecordProcessor recordProcessor;
    private Table mockDdbExportTimeTable;

    @BeforeMethod
    public void before() throws Exception {
        // mock Config - For branch coverage, make progress report period 2
        Config mockConfig = mock(Config.class);
        when(mockConfig.getInt(BridgeExporterRecordProcessor.CONFIG_KEY_RECORD_LOOP_DELAY_MILLIS)).thenReturn(0);
        when(mockConfig.getInt(BridgeExporterRecordProcessor.CONFIG_KEY_RECORD_LOOP_PROGRESS_REPORT_PERIOD))
                .thenReturn(2);
        when(mockConfig.get(BridgeExporterUtil.CONFIG_KEY_TIME_ZONE_NAME))
                .thenReturn("America/Los_Angeles");

        // mock Synapse Helper - For this test, Synapse is up and writable.
        SynapseHelper mockSynapseHelper = mock(SynapseHelper.class);
        when(mockSynapseHelper.isSynapseWritable()).thenReturn(true);

        // mocks
        mockDdbRecordTable = mock(Table.class);
        mockFileHelper = new InMemoryFileHelper();
        mockManager = mock(ExportWorkerManager.class);
        mockMetricsHelper = mock(MetricsHelper.class);
        mockRecordFilterHelper = mock(RecordFilterHelper.class);
        mockRecordIdFactory = mock(RecordIdSourceFactory.class);
        mockDdbExportTimeTable = mock(Table.class);

        // set up record processor
        recordProcessor = spy(new BridgeExporterRecordProcessor());
        recordProcessor.setConfig(mockConfig);
        recordProcessor.setDdbRecordTable(mockDdbRecordTable);
        recordProcessor.setFileHelper(mockFileHelper);
        recordProcessor.setMetricsHelper(mockMetricsHelper);
        recordProcessor.setRecordFilterHelper(mockRecordFilterHelper);
        recordProcessor.setRecordIdSourceFactory(mockRecordIdFactory);
        recordProcessor.setSynapseHelper(mockSynapseHelper);
        recordProcessor.setWorkerManager(mockManager);
        recordProcessor.setDdbExportTimeTable(mockDdbExportTimeTable);
    }

    @Test
    public void test() throws Exception {
        // 5 records:
        // * success
        // * filtered
        // * missing
        // * error
        // * success again

        // mock DDB record table - We don't look inside any of these records, so for the purposes of this test, just
        // make dummy DDB record items with no content.
        Item dummySuccessRecord1 = new Item();
        Item dummyFilteredRecord = new Item();
        Item dummyErrorRecord = new Item();
        Item dummySuccessRecord2 = new Item();

        when(mockDdbRecordTable.getItem("id", "success-record-1")).thenReturn(dummySuccessRecord1);
        when(mockDdbRecordTable.getItem("id", "filtered-record")).thenReturn(dummyFilteredRecord);
        when(mockDdbRecordTable.getItem("id", "error-record")).thenReturn(dummyErrorRecord);
        when(mockDdbRecordTable.getItem("id", "success-record-2")).thenReturn(dummySuccessRecord2);

        // mock record filter helper - Only mock the filtered record. All the others will return false by default in
        // Mockito.
        ArgumentCaptor<Metrics> recordFilterMetricsCaptor = ArgumentCaptor.forClass(Metrics.class);
        when(mockRecordFilterHelper.shouldExcludeRecord(recordFilterMetricsCaptor.capture(), same(REQUEST),
                same(dummyFilteredRecord))).thenReturn(true);

        // mock record ID factory
        List<String> recordIdList = ImmutableList.of("success-record-1", "filtered-record", "missing-record",
                "error-record", "success-record-2");
        when(mockRecordIdFactory.getRecordSourceForRequest(REQUEST)).thenReturn(new RecordAndStudyListHolder(recordIdList, ImmutableMap.of(), DateTime.now()));

        // mock export worker manager - Only mock error record. The others will just no-op by default in Mockito.
        doThrow(IOException.class).when(mockManager).addSubtaskForRecord(any(ExportTask.class),
                same(dummyErrorRecord));

        // execute
        recordProcessor.processRecordsForRequest(REQUEST);

        // validate metrics helper - Validate that we're passing the same set of metrics to it for each record, and
        // that we're calling publishMetrics() at the end with the same metrics object.
        ArgumentCaptor<Metrics> metricsHelperArgCaptor = ArgumentCaptor.forClass(Metrics.class);
        verify(mockMetricsHelper).captureMetricsForRecord(metricsHelperArgCaptor.capture(), same(dummySuccessRecord1));
        verify(mockMetricsHelper).captureMetricsForRecord(metricsHelperArgCaptor.capture(), same(dummyErrorRecord));
        verify(mockMetricsHelper).captureMetricsForRecord(metricsHelperArgCaptor.capture(), same(dummySuccessRecord2));
        verify(mockMetricsHelper).publishMetrics(metricsHelperArgCaptor.capture());
        verifyNoMoreInteractions(mockMetricsHelper);

        List<Metrics> metricsHelperArgList = metricsHelperArgCaptor.getAllValues();
        assertEquals(metricsHelperArgList.size(), 4);
        assertSame(metricsHelperArgList.get(1), metricsHelperArgList.get(0));
        assertSame(metricsHelperArgList.get(2), metricsHelperArgList.get(0));
        assertSame(metricsHelperArgList.get(3), metricsHelperArgList.get(0));

        // validate worker manager - Similarly, validate that we're passing the same task for each record, and that
        // we're calling endOfStream() with that same task at the end.
        ArgumentCaptor<ExportTask> managerTaskArgCaptor = ArgumentCaptor.forClass(ExportTask.class);
        verify(mockManager).addSubtaskForRecord(managerTaskArgCaptor.capture(), same(dummySuccessRecord1));
        verify(mockManager).addSubtaskForRecord(managerTaskArgCaptor.capture(), same(dummyErrorRecord));
        verify(mockManager).addSubtaskForRecord(managerTaskArgCaptor.capture(), same(dummySuccessRecord2));
        verify(mockManager).endOfStream(managerTaskArgCaptor.capture());
        verifyNoMoreInteractions(mockManager);

        List<ExportTask> managerTaskArgList = managerTaskArgCaptor.getAllValues();
        assertEquals(managerTaskArgList.size(), 4);
        assertSame(managerTaskArgList.get(1), managerTaskArgList.get(0));
        assertSame(managerTaskArgList.get(2), managerTaskArgList.get(0));
        assertSame(managerTaskArgList.get(3), managerTaskArgList.get(0));

        // verify that we marked the task as success
        verify(recordProcessor).setTaskSuccess(any());

        // validate record filter metrics is the same as the one passed to the metrics helper
        Metrics recordFilterMetrics = recordFilterMetricsCaptor.getValue();
        assertSame(recordFilterMetrics, metricsHelperArgList.get(0));

        // validate that we cleaned up all our files
        assertTrue(mockFileHelper.isEmpty());
    }

    @Test
    public void endOfStreamThrows() throws Exception {
        // Only need 1 test record this time.

        // mock DDB record table and record ID factory
        when(mockDdbRecordTable.getItem("id", "dummy-record")).thenReturn(new Item());
        when(mockRecordIdFactory.getRecordSourceForRequest(REQUEST)).thenReturn(new RecordAndStudyListHolder(ImmutableList.of("dummy-record"),
                ImmutableMap.of(), DateTime.now()));

        // ExportWorkerManager throws in endOfStream()
        doThrow(RestartBridgeExporterException.class).when(mockManager).endOfStream(any());

        // execute (this will throw)
        try {
            recordProcessor.processRecordsForRequest(REQUEST);
            fail("expected exception");
        } catch (RestartBridgeExporterException ex) {
            // expected exception
        }

        // verify that we're NOT marking the task as success
        verify(recordProcessor, never()).setTaskSuccess(any());
    }

    @Test
    public void updateExportTimeTable() throws Exception {
        Map<String, DateTime> testStudyIdsToUpdate = new LinkedHashMap<>();
        testStudyIdsToUpdate.put("id1", new DateTime());
        testStudyIdsToUpdate.put("id2", new DateTime());

        Item dummySuccessRecord1 = new Item();
        when(mockDdbRecordTable.getItem("id", "success-record-1")).thenReturn(dummySuccessRecord1);

        // mock record ID factory
        List<String> recordIdList = ImmutableList.of("success-record-1");
        when(mockRecordIdFactory.getRecordSourceForRequest(REQUEST)).thenReturn(new RecordAndStudyListHolder(recordIdList, testStudyIdsToUpdate, END_DATE_TIME));

        // execute
        recordProcessor.processRecordsForRequest(REQUEST);

        // verify that we marked the task as success
        verify(recordProcessor).setTaskSuccess(any());

        ArgumentCaptor<Item> itemArgumentCaptor = ArgumentCaptor.forClass(Item.class);
        verify(mockDdbExportTimeTable, times(2)).putItem(itemArgumentCaptor.capture());

        List<Item> items = itemArgumentCaptor.getAllValues();
        Item item1 = items.get(0);
        assertEquals(item1.get(STUDY_ID), "id1");
        assertEquals(item1.getLong(LAST_EXPORT_DATE_TIME), END_DATE_TIME.getMillis());
        Item item2 = items.get(1);
        assertEquals(item2.get(STUDY_ID), "id2");
        assertEquals(item2.getLong(LAST_EXPORT_DATE_TIME), END_DATE_TIME.getMillis());

        // validate that we cleaned up all our files
        assertTrue(mockFileHelper.isEmpty());
    }
}
