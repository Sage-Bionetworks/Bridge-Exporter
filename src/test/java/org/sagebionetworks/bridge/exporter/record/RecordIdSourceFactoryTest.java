package org.sagebionetworks.bridge.exporter.record;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.sagebionetworks.bridge.exporter.record.RecordIdSourceFactory.LAST_EXPORT_DATE_TIME;
import static org.sagebionetworks.bridge.exporter.record.RecordIdSourceFactory.STUDY_ID;
import static org.sagebionetworks.bridge.exporter.record.RecordIdSourceFactory.UPLOADED_ON;
import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.KeyConditions;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.dynamodb.DynamoQueryHelper;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.s3.S3Helper;

public class RecordIdSourceFactoryTest {
    private static final String UPLOAD_DATE = "2015-11-11";
    private static final String UPLOAD_START_DATE_TIME = "2015-11-11T00:00:00Z";
    private static final String UPLOAD_END_DATE_TIME = "2015-11-11T23:59:59Z";
    private static final String EXPORT_TIME_TABLE_NAME = "exportTime";

    private static final DateTime UPLOAD_START_DATE_TIME_OBJ = DateTime.parse(UPLOAD_START_DATE_TIME);
    private static final DateTime UPLOAD_END_DATE_TIME_OBJ = DateTime.parse(UPLOAD_END_DATE_TIME);

    @Test
    public void fromDdbWithLastExportDateTime() throws Exception {
        // mock exportTime ddb table with mock items
        Table mockExportTimetable = mock(Table.class);
        when(mockExportTimetable.getTableName()).thenReturn(EXPORT_TIME_TABLE_NAME);

        // mock ddb client with scan result as last export date time
        AmazonDynamoDBClient mockDdbClient = mock(AmazonDynamoDBClient.class);

        Map<String, AttributeValue> exportTimeItemMap_1 = ImmutableMap.of(STUDY_ID, new AttributeValue().withS("ddb-foo"), LAST_EXPORT_DATE_TIME, new AttributeValue().withN("123"));
        Map<String, AttributeValue> exportTimeItemMap_2 = ImmutableMap.of(STUDY_ID, new AttributeValue().withS("ddb-bar"), LAST_EXPORT_DATE_TIME, new AttributeValue().withN("123"));
        List<Map<String, AttributeValue>> exportTimeItemList = ImmutableList.of(exportTimeItemMap_1, exportTimeItemMap_2);
        ScanResult scanResult = new ScanResult();
        scanResult.setItems(exportTimeItemList);
        ScanRequest scanRequest = new ScanRequest().withTableName(EXPORT_TIME_TABLE_NAME);
        when(mockDdbClient.scan(eq(scanRequest))).thenReturn(scanResult);

        // mock DDB - The actual table index is just a dummy, since the query helper does all the real work.
        List<Item> ddbItemList = ImmutableList.of(
                new Item().withString("id", "ddb-foo")
                        .withString(STUDY_ID, "ddb-foo")
                        .withLong(UPLOADED_ON, 144L),
                new Item().withString("id", "ddb-bar")
                        .withString(STUDY_ID, "ddb-bar")
                        .withLong(UPLOADED_ON, 155L),
                new Item().withString("id", "ddb-baz")
                        .withString(STUDY_ID, "ddb-baz")
                        .withLong(UPLOADED_ON, 111L));

        Index mockRecordIndex = mock(Index.class);
        DynamoQueryHelper mockQueryHelper = mock(DynamoQueryHelper.class);
        when(mockQueryHelper.query(mockRecordIndex, "uploadDate", UPLOAD_DATE)).thenReturn(ddbItemList);

        // set up factory
        RecordIdSourceFactory factory = new RecordIdSourceFactory();
        factory.setDdbQueryHelper(mockQueryHelper);
        factory.setDdbRecordUploadDateIndex(mockRecordIndex);
        factory.setDdbExportTimeTable(mockExportTimetable);
        factory.setDdbClientScan(mockDdbClient);

        // execute and validate
        BridgeExporterRequest request = new BridgeExporterRequest.Builder().withDate(LocalDate.parse(UPLOAD_DATE))
                .withStartDateTime(UPLOAD_START_DATE_TIME_OBJ)
                .withEndDateTime(UPLOAD_END_DATE_TIME_OBJ)
                .build();
        Iterable<String> recordIdIter = factory.getRecordSourceForRequest(request);

        verify(mockDdbClient).scan(eq(scanRequest));
        List<String> recordIdList = ImmutableList.copyOf(recordIdIter);
        assertEquals(recordIdList.size(), 2); // only output records in given time range
        assertEquals(recordIdList.get(0), "ddb-foo");
        assertEquals(recordIdList.get(1), "ddb-bar");
    }

    @Test
    public void fromDdbWithoutLastExportDateTime() throws Exception {
        // mock exportTime ddb table with empty items
        Table mockExportTimetable = mock(Table.class);
        when(mockExportTimetable.getTableName()).thenReturn(EXPORT_TIME_TABLE_NAME);

        // mock ddb client with scan result as last export date time
        AmazonDynamoDBClient mockDdbClient = mock(AmazonDynamoDBClient.class);

        List<Map<String, AttributeValue>> exportTimeItemList = ImmutableList.of();
        ScanResult scanResult = new ScanResult();
        scanResult.setItems(exportTimeItemList);
        ScanRequest scanRequest = new ScanRequest().withTableName(EXPORT_TIME_TABLE_NAME);
        when(mockDdbClient.scan(eq(scanRequest))).thenReturn(scanResult);

        // mock DDB - The actual table index is just a dummy, since the query helper does all the real work.
        List<Item> ddbItemList = ImmutableList.of(
                new Item().withString("id", "ddb-foo")
                        .withString(STUDY_ID, "ddb-foo")
                        .withLong(UPLOADED_ON, UPLOAD_START_DATE_TIME_OBJ.withHourOfDay(1).getMillis()),
                new Item().withString("id", "ddb-bar")
                        .withString(STUDY_ID, "ddb-bar")
                        .withLong(UPLOADED_ON, UPLOAD_START_DATE_TIME_OBJ.withHourOfDay(2).getMillis()),
                new Item().withString("id", "ddb-baz")
                        .withString(STUDY_ID, "ddb-baz")
                        .withLong(UPLOADED_ON, UPLOAD_START_DATE_TIME_OBJ.minusHours(1).getMillis()));

        Index mockRecordIndex = mock(Index.class);
        DynamoQueryHelper mockQueryHelper = mock(DynamoQueryHelper.class);
        when(mockQueryHelper.query(mockRecordIndex, "uploadDate", UPLOAD_DATE)).thenReturn(ddbItemList);

        // set up factory
        RecordIdSourceFactory factory = new RecordIdSourceFactory();
        factory.setDdbQueryHelper(mockQueryHelper);
        factory.setDdbRecordUploadDateIndex(mockRecordIndex);
        factory.setDdbExportTimeTable(mockExportTimetable);
        factory.setDdbClientScan(mockDdbClient);

        // execute and validate
        BridgeExporterRequest request = new BridgeExporterRequest.Builder().withDate(LocalDate.parse(UPLOAD_DATE))
                .withStartDateTime(DateTime.parse(UPLOAD_START_DATE_TIME))
                .withEndDateTime(DateTime.parse(UPLOAD_END_DATE_TIME))
                .build();
        Iterable<String> recordIdIter = factory.getRecordSourceForRequest(request);

        verify(mockDdbClient).scan(eq(scanRequest));
        List<String> recordIdList = ImmutableList.copyOf(recordIdIter);
        assertEquals(recordIdList.size(), 2);
        assertEquals(recordIdList.get(0), "ddb-foo");
        assertEquals(recordIdList.get(1), "ddb-bar");
    }

    @Test
    public void fromDdbWithReExport() throws Exception {
        // mock exportTime ddb table with mock items
        Table mockExportTimetable = mock(Table.class);
        when(mockExportTimetable.getTableName()).thenReturn(EXPORT_TIME_TABLE_NAME);

        // mock ddb client with scan result as last export date time
        AmazonDynamoDBClient mockDdbClient = mock(AmazonDynamoDBClient.class);

        // mock DDB - The actual table index is just a dummy, since the query helper does all the real work.
        List<Item> ddbItemList = ImmutableList.of(
                new Item().withString("id", "ddb-foo")
                        .withString(STUDY_ID, "ddb-foo")
                        .withLong(UPLOADED_ON, UPLOAD_START_DATE_TIME_OBJ.getMillis()),
                new Item().withString("id", "ddb-bar")
                        .withString(STUDY_ID, "ddb-bar")
                        .withLong(UPLOADED_ON, UPLOAD_START_DATE_TIME_OBJ.getMillis()),
                new Item().withString("id", "ddb-baz")
                        .withString(STUDY_ID, "ddb-baz")
                        .withLong(UPLOADED_ON, 155L));

        Index mockRecordIndex = mock(Index.class);
        DynamoQueryHelper mockQueryHelper = mock(DynamoQueryHelper.class);
        when(mockQueryHelper.query(mockRecordIndex, "uploadDate", UPLOAD_DATE)).thenReturn(ddbItemList);

        // set up factory
        RecordIdSourceFactory factory = new RecordIdSourceFactory();
        factory.setDdbQueryHelper(mockQueryHelper);
        factory.setDdbRecordUploadDateIndex(mockRecordIndex);
        factory.setDdbExportTimeTable(mockExportTimetable);
        factory.setDdbClientScan(mockDdbClient);

        // execute and validate
        BridgeExporterRequest request = new BridgeExporterRequest.Builder().withDate(LocalDate.parse(UPLOAD_DATE))
                .withStartDateTime(UPLOAD_START_DATE_TIME_OBJ)
                .withEndDateTime(UPLOAD_END_DATE_TIME_OBJ)
                .withReExport(true)
                .build();
        Iterable<String> recordIdIter = factory.getRecordSourceForRequest(request);

        verifyZeroInteractions(mockDdbClient);
        List<String> recordIdList = ImmutableList.copyOf(recordIdIter);
        assertEquals(recordIdList.size(), 2); // only output records in given time range
        assertEquals(recordIdList.get(0), "ddb-foo");
        assertEquals(recordIdList.get(1), "ddb-bar");
    }

    @Test
    public void fromDdbWithWhitelistWithLastExportDateTime() throws Exception {
        BridgeExporterRequest.Builder requestBuilder = new BridgeExporterRequest.Builder().withDate(LocalDate.parse(
                "2016-05-09"));
        fromDdbWithWhitelist(requestBuilder, UPLOAD_START_DATE_TIME_OBJ.minusHours(1).getMillis(),
                UPLOAD_END_DATE_TIME_OBJ.getMillis() - 1, true, false);
    }

    @Test
    public void fromDdbWithWhitelistWithoutLastExportDateTime() throws Exception {
        BridgeExporterRequest.Builder requestBuilder = new BridgeExporterRequest.Builder().withDate(LocalDate.parse(
                "2016-05-09"));
        fromDdbWithWhitelist(requestBuilder, UPLOAD_START_DATE_TIME_OBJ.getMillis(),
                UPLOAD_END_DATE_TIME_OBJ.getMillis() - 1, false, false);
    }

    @Test
    public void fromDdbWithWhitelistWithReExport() throws Exception {
        BridgeExporterRequest.Builder requestBuilder = new BridgeExporterRequest.Builder().withDate(LocalDate.parse(
                "2016-05-09"));
        fromDdbWithWhitelist(requestBuilder, UPLOAD_START_DATE_TIME_OBJ.minusHours(1).getMillis(),
                UPLOAD_END_DATE_TIME_OBJ.getMillis() - 1, true, true);
    }

    private static void fromDdbWithWhitelist(BridgeExporterRequest.Builder requestBuilder, long expectedStartMillis,
            long expectedEndMillis, boolean withLastExportDateTime, boolean isReExport) throws Exception {
        // mock exportTime ddb table with mock items
        Table mockExportTimetable = mock(Table.class);

        Item barItem = new Item().withString(STUDY_ID, "bar-study").withLong(LAST_EXPORT_DATE_TIME, expectedStartMillis);
        Item fooItem = new Item().withString(STUDY_ID, "foo-study").withLong(LAST_EXPORT_DATE_TIME, expectedStartMillis);

        when(mockExportTimetable.getTableName()).thenReturn(EXPORT_TIME_TABLE_NAME);

        if (withLastExportDateTime) {
            when(mockExportTimetable.getItem(STUDY_ID, "foo-study")).thenReturn(fooItem);
            when(mockExportTimetable.getItem(STUDY_ID, "bar-study")).thenReturn(barItem);
        }

        // mock DDB
        List<Item> barStudyItemList = ImmutableList.of(new Item().withString("id", "bar-1"),
                new Item().withString("id", "bar-2"));
        List<Item> fooStudyItemList = ImmutableList.of(new Item().withString("id", "foo-1"),
                new Item().withString("id", "foo-2"));

        Index mockRecordIndex = mock(Index.class);
        DynamoQueryHelper mockQueryHelper = mock(DynamoQueryHelper.class);

        ArgumentCaptor<RangeKeyCondition> barRangeKeyCaptor = ArgumentCaptor.forClass(RangeKeyCondition.class);
        when(mockQueryHelper.query(same(mockRecordIndex), eq("studyId"), eq("bar-study"), barRangeKeyCaptor.capture()))
                .thenReturn(barStudyItemList);

        ArgumentCaptor<RangeKeyCondition> fooRangeKeyCaptor = ArgumentCaptor.forClass(RangeKeyCondition.class);
        when(mockQueryHelper.query(same(mockRecordIndex), eq("studyId"), eq("foo-study"), fooRangeKeyCaptor.capture()))
                .thenReturn(fooStudyItemList);

        // set up factory
        RecordIdSourceFactory factory = new RecordIdSourceFactory();
        factory.setConfig(mockConfig());
        factory.setDdbQueryHelper(mockQueryHelper);
        factory.setDdbRecordStudyUploadedOnIndex(mockRecordIndex);
        factory.setDdbExportTimeTable(mockExportTimetable);

        // finish building request - Use a TreeSet for the study whitelist, so we can get a deterministic test.
        // (ImmutableSet preserves the iteration order.)
        Set<String> studyWhitelist = new TreeSet<>();
        studyWhitelist.add("bar-study");
        studyWhitelist.add("foo-study");
        BridgeExporterRequest request = requestBuilder
                .withStudyWhitelist(studyWhitelist)
                .withStartDateTime(UPLOAD_START_DATE_TIME_OBJ)
                .withEndDateTime(UPLOAD_END_DATE_TIME_OBJ)
                .withReExport(isReExport).build();

        // execute and validate
        Iterable<String> recordIdIter = factory.getRecordSourceForRequest(request);
        List<String> recordIdList = ImmutableList.copyOf(recordIdIter);
        assertEquals(recordIdList.size(), 4);
        assertEquals(recordIdList.get(0), "bar-1");
        assertEquals(recordIdList.get(1), "bar-2");
        assertEquals(recordIdList.get(2), "foo-1");
        assertEquals(recordIdList.get(3), "foo-2");

        // validate range key queries
        if (isReExport) {
            validateRangeKey(barRangeKeyCaptor.getValue(), UPLOAD_START_DATE_TIME_OBJ.getMillis(), expectedEndMillis);
            validateRangeKey(fooRangeKeyCaptor.getValue(), UPLOAD_START_DATE_TIME_OBJ.getMillis(), expectedEndMillis);
        } else {
            validateRangeKey(barRangeKeyCaptor.getValue(), expectedStartMillis, expectedEndMillis);
            validateRangeKey(fooRangeKeyCaptor.getValue(), expectedStartMillis, expectedEndMillis);
        }
    }

    private static void validateRangeKey(RangeKeyCondition rangeKey, long expectedStartMillis,
            long expectedEndMillis) {
        assertEquals(rangeKey.getAttrName(), "uploadedOn");
        assertEquals(rangeKey.getKeyCondition(), KeyConditions.BETWEEN);

        Object[] rangeKeyValueArray = rangeKey.getValues();
        assertEquals(rangeKeyValueArray.length, 2);
        assertEquals(rangeKeyValueArray[0], expectedStartMillis);
        assertEquals(rangeKeyValueArray[1], expectedEndMillis);
    }

    @Test
    public void fromS3Override() throws Exception {
        // mock S3
        List<String> s3Lines = ImmutableList.of("s3-foo", "s3-bar", "s3-baz");
        S3Helper mockS3Helper = mock(S3Helper.class);
        when(mockS3Helper.readS3FileAsLines("dummy-override-bucket", "dummy-override-file")).thenReturn(s3Lines);

        // set up factory
        RecordIdSourceFactory factory = new RecordIdSourceFactory();
        factory.setConfig(mockConfig());
        factory.setS3Helper(mockS3Helper);

        // execute and validate
        BridgeExporterRequest request = new BridgeExporterRequest.Builder()
                .withRecordIdS3Override("dummy-override-file").build();
        Iterable<String> recordIdIter = factory.getRecordSourceForRequest(request);

        List<String> recordIdList = ImmutableList.copyOf(recordIdIter);
        assertEquals(recordIdList.size(), 3);
        assertEquals(recordIdList.get(0), "s3-foo");
        assertEquals(recordIdList.get(1), "s3-bar");
        assertEquals(recordIdList.get(2), "s3-baz");
    }

    private static Config mockConfig() {
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(BridgeExporterUtil.CONFIG_KEY_RECORD_ID_OVERRIDE_BUCKET))
                .thenReturn("dummy-override-bucket");
        when(mockConfig.get(BridgeExporterUtil.CONFIG_KEY_TIME_ZONE_NAME)).thenReturn("America/Los_Angeles");
        return mockConfig;
    }
}
