package org.sagebionetworks.bridge.exporter.record;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.sagebionetworks.bridge.exporter.record.RecordIdSourceFactory.IDENTIFIER;
import static org.sagebionetworks.bridge.exporter.record.RecordIdSourceFactory.LAST_EXPORT_DATE_TIME;
import static org.sagebionetworks.bridge.exporter.record.RecordIdSourceFactory.STUDY_ID;
import static org.testng.Assert.assertEquals;

import java.util.LinkedHashSet;
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
import org.joda.time.DateTimeUtils;
import org.joda.time.LocalDate;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.dynamodb.DynamoQueryHelper;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.s3.S3Helper;

public class RecordIdSourceFactoryTest {
    private static final String UPLOAD_DATE = "2016-05-09";
    private static final String UPLOAD_START_DATE_TIME = "2016-05-09T00:00:00.000-0700";
    private static final String UPLOAD_END_DATE_TIME = "2016-05-09T23:59:59.999-0700";
    private static final String EXPORT_TIME_TABLE_NAME = "exportTime";
    private static final String STUDY_TABLE_NAME = "Study";
    private static final String CURRENT_DATE = "2017-03-22";

    private static final DateTime UPLOAD_START_DATE_TIME_OBJ = DateTime.parse(UPLOAD_START_DATE_TIME);
    private static final DateTime UPLOAD_END_DATE_TIME_OBJ = DateTime.parse(UPLOAD_END_DATE_TIME);
    private static final LocalDate UPLOAD_DATE_OBJ = LocalDate.parse(UPLOAD_DATE);
    private static final DateTime CURRENT_DATE_OBJ = DateTime.parse(CURRENT_DATE);

    @BeforeClass
    public void setup() {
        // fix current datetime to a static value for testing
        DateTimeUtils.setCurrentMillisFixed(CURRENT_DATE_OBJ.getMillis());
    }

    @Test
    public void fromDdb() throws Exception {
        // mock DDB - The actual table index is just a dummy, since the query helper does all the real work.
        List<Item> ddbItemList = ImmutableList.of(new Item().withString("id", "ddb-foo"),
                new Item().withString("id", "ddb-bar"), new Item().withString("id", "ddb-baz"));

        Index mockRecordIndex = mock(Index.class);
        DynamoQueryHelper mockQueryHelper = mock(DynamoQueryHelper.class);
        when(mockQueryHelper.query(mockRecordIndex, "uploadDate", UPLOAD_DATE)).thenReturn(ddbItemList);

        // mock ddb client with scan result as last export date time
        AmazonDynamoDBClient mockDdbClient = mock(AmazonDynamoDBClient.class);

        // mock study table and study id list
        Table mockStudyTable = mock(Table.class);
        when(mockStudyTable.getTableName()).thenReturn(STUDY_TABLE_NAME);

        List<Map<String, AttributeValue>> studyIdList = ImmutableList.of(
                ImmutableMap.of(IDENTIFIER, new AttributeValue().withS("ddb-foo")),
                ImmutableMap.of(IDENTIFIER, new AttributeValue().withS("ddb-bar"))
        );

        ScanResult scanResult = new ScanResult();
        scanResult.setItems(studyIdList);
        ScanRequest scanRequest = new ScanRequest().withTableName(STUDY_TABLE_NAME);
        when(mockDdbClient.scan(eq(scanRequest))).thenReturn(scanResult);

        // mock exportTime ddb table with mock items
        Table mockExportTimeTable = mock(Table.class);
        when(mockExportTimeTable.getTableName()).thenReturn(EXPORT_TIME_TABLE_NAME);


        // set up factory
        RecordIdSourceFactory factory = new RecordIdSourceFactory();
        factory.setDdbQueryHelper(mockQueryHelper);
        factory.setDdbRecordUploadDateIndex(mockRecordIndex);
        factory.setDdbExportTimeTable(mockExportTimeTable);
        factory.setDdbStudyTable(mockStudyTable);
        factory.setDdbClientScan(mockDdbClient);
        // execute and validate
        BridgeExporterRequest request = new BridgeExporterRequest.Builder().withDate(UPLOAD_DATE_OBJ)
                .withStartDateTime(UPLOAD_START_DATE_TIME_OBJ)
                .withEndDateTime(UPLOAD_END_DATE_TIME_OBJ)
                .withExportType(ExportType.DAILY)
                .build();
        Iterable<String> recordIdIter = factory.getRecordSourceForRequest(request).getRecordIdSource();

        List<String> recordIdList = ImmutableList.copyOf(recordIdIter);
        assertEquals(recordIdList.size(), 3);
        assertEquals(recordIdList.get(0), "ddb-foo");
        assertEquals(recordIdList.get(1), "ddb-bar");
        assertEquals(recordIdList.get(2), "ddb-baz");
    }

    @Test
    public void fromDdbWithWhitelistAndDate() throws Exception {
        BridgeExporterRequest.Builder requestBuilder = new BridgeExporterRequest.Builder().withDate(LocalDate.parse(
                "2016-05-09"));
        fromDdbWithWhitelist(requestBuilder, DateTime.parse("2016-05-09T00:00:00.000-0700").getMillis(),
                DateTime.parse("2016-05-09T23:59:59.999-0700").getMillis());
    }

    @Test
    public void fromDdbWithWhitelistAndStartAndEndDateTime() throws Exception {
        DateTime startDateTime = UPLOAD_START_DATE_TIME_OBJ;
        DateTime endDateTime = UPLOAD_END_DATE_TIME_OBJ;

        BridgeExporterRequest.Builder requestBuilder = new BridgeExporterRequest.Builder()
                .withStartDateTime(startDateTime).withEndDateTime(endDateTime);

        fromDdbWithWhitelist(requestBuilder, startDateTime.getMillis(), endDateTime.getMillis() - 1);
    }

    private static void fromDdbWithWhitelist(BridgeExporterRequest.Builder requestBuilder, long expectedStartMillis,
            long expectedEndMillis) throws Exception {
        // mock exportTime ddb table with mock items
        Table mockExportTimeTable = mock(Table.class);
        when(mockExportTimeTable.getTableName()).thenReturn(EXPORT_TIME_TABLE_NAME);

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
        factory.setDdbExportTimeTable(mockExportTimeTable);

        // finish building request - Use a TreeSet for the study whitelist, so we can get a deterministic test.
        // (ImmutableSet preserves the iteration order.)
        Set<String> studyWhitelist = new TreeSet<>();
        studyWhitelist.add("bar-study");
        studyWhitelist.add("foo-study");
        BridgeExporterRequest request = requestBuilder.withStudyWhitelist(studyWhitelist)
                .withStartDateTime(UPLOAD_START_DATE_TIME_OBJ)
                .withEndDateTime(UPLOAD_END_DATE_TIME_OBJ)
                .withExportType(ExportType.DAILY).build();

        // execute and validate
        Iterable<String> recordIdIter = factory.getRecordSourceForRequest(request).getRecordIdSource();
        List<String> recordIdList = ImmutableList.copyOf(recordIdIter);
        assertEquals(recordIdList.size(), 4);
        assertEquals(recordIdList.get(0), "bar-1");
        assertEquals(recordIdList.get(1), "bar-2");
        assertEquals(recordIdList.get(2), "foo-1");
        assertEquals(recordIdList.get(3), "foo-2");

        // validate range key queries
        validateRangeKey(barRangeKeyCaptor.getValue(), expectedStartMillis, expectedEndMillis);
        validateRangeKey(fooRangeKeyCaptor.getValue(), expectedStartMillis, expectedEndMillis);
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
        Iterable<String> recordIdIter = factory.getRecordSourceForRequest(request).getRecordIdSource();

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

    @Test
    public void fromDdbWithLastExportDateTime() throws Exception {
        fromDdb(false, true, false, ExportType.DAILY);
        fromDdb(false, true, false, ExportType.HOURLY);
        fromDdb(false, true, false, ExportType.INSTANT);
    }

    @Test
    public void fromDdbWithoutLastExportDateTime() throws Exception {
        fromDdb(false, false, false, ExportType.DAILY);
        fromDdb(false, false, false, ExportType.HOURLY);
        fromDdb(false, false, false, ExportType.INSTANT);
    }

    @Test
    public void fromDdbIgnoreLastExportTime() throws Exception {
        // doesn't care about if it has last export date time for re export
        fromDdb(true, true, false, ExportType.DAILY);
        fromDdb(true, false, false, ExportType.DAILY);
        fromDdb(true, true, false, ExportType.HOURLY);
        fromDdb(true, false, false, ExportType.HOURLY);
        fromDdb(true, true, false, ExportType.INSTANT);
        fromDdb(true, false, false, ExportType.INSTANT);
    }

    @Test
    public void fromDdbWithWhitelistWithLastExportDateTime() throws Exception {
        fromDdb(false, true, true, ExportType.DAILY);
        fromDdb(false, true, true, ExportType.HOURLY);
        fromDdb(false, true, true, ExportType.INSTANT);
    }

    @Test
    public void fromDdbWithWhitelistWithoutLastExportDateTime() throws Exception {
        fromDdb(false, false, true, ExportType.DAILY);
        fromDdb(false, false, true, ExportType.HOURLY);
        fromDdb(false, false, true, ExportType.INSTANT);
    }

    @Test
    public void fromDdbWithWhitelistIgnoreLastExportTime() throws Exception {
        fromDdb(true, true, true, ExportType.DAILY);
        fromDdb(true, false, true, ExportType.DAILY);
        fromDdb(true, true, true, ExportType.HOURLY);
        fromDdb(true, false, true, ExportType.HOURLY);
        fromDdb(true, true, true, ExportType.INSTANT);
        fromDdb(true, false, true, ExportType.INSTANT);
    }

    private static void fromDdb(boolean ignoreLastExportTime, boolean withLastExportDateTime, boolean withWhitelist, ExportType exportType) throws Exception{
        // mock ddb client with scan result as last export date time
        AmazonDynamoDBClient mockDdbClient = mock(AmazonDynamoDBClient.class);

        // mock study table and study id list
        Table mockStudyTable = mock(Table.class);
        when(mockStudyTable.getTableName()).thenReturn(STUDY_TABLE_NAME);

        List<Map<String, AttributeValue>> studyIdList = ImmutableList.of(
                ImmutableMap.of(IDENTIFIER, new AttributeValue().withS("ddb-foo")),
                ImmutableMap.of(IDENTIFIER, new AttributeValue().withS("ddb-bar"))
        );

        ScanResult scanResult = new ScanResult();
        scanResult.setItems(studyIdList);
        ScanRequest scanRequest = new ScanRequest().withTableName(STUDY_TABLE_NAME);
        when(mockDdbClient.scan(eq(scanRequest))).thenReturn(scanResult);

        // mock exportTime ddb table with mock items
        Table mockExportTimeTable = mock(Table.class);
        when(mockExportTimeTable.getTableName()).thenReturn(EXPORT_TIME_TABLE_NAME);

        if (withLastExportDateTime) {
            Item fooItem = new Item().withString(STUDY_ID, "ddb-foo").withLong(LAST_EXPORT_DATE_TIME, 123L);
            Item barItem = new Item().withString(STUDY_ID, "ddb-bar").withLong(LAST_EXPORT_DATE_TIME, 123L);
            when(mockExportTimeTable.getItem(STUDY_ID, "ddb-foo")).thenReturn(fooItem);
            when(mockExportTimeTable.getItem(STUDY_ID, "ddb-bar")).thenReturn(barItem);
        }

        // mock DDB
        List<Item> fooStudyItemList = ImmutableList.of(new Item().withString("id", "foo-1"),
                new Item().withString("id", "foo-2"));
        List<Item> barStudyItemList = ImmutableList.of(new Item().withString("id", "bar-1"),
                new Item().withString("id", "bar-2"));

        Index mockRecordIndex = mock(Index.class);
        DynamoQueryHelper mockQueryHelper = mock(DynamoQueryHelper.class);

        ArgumentCaptor<RangeKeyCondition> barRangeKeyCaptor = ArgumentCaptor.forClass(RangeKeyCondition.class);
        when(mockQueryHelper.query(same(mockRecordIndex), eq(STUDY_ID), eq("ddb-bar"), barRangeKeyCaptor.capture()))
                .thenReturn(barStudyItemList);

        when(mockQueryHelper.query(same(mockRecordIndex), eq("uploadDate"), eq(UPLOAD_DATE)))
                .thenReturn(barStudyItemList);

        ArgumentCaptor<RangeKeyCondition> fooRangeKeyCaptor = ArgumentCaptor.forClass(RangeKeyCondition.class);
        when(mockQueryHelper.query(same(mockRecordIndex), eq(STUDY_ID), eq("ddb-foo"), fooRangeKeyCaptor.capture()))
                .thenReturn(fooStudyItemList);

        when(mockQueryHelper.query(same(mockRecordIndex), eq("uploadDate"), eq(UPLOAD_DATE)))
                .thenReturn(fooStudyItemList);

        // set up factory
        RecordIdSourceFactory factory = new RecordIdSourceFactory();
        factory.setDdbQueryHelper(mockQueryHelper);
        factory.setConfig(mockConfig());
        factory.setDdbRecordStudyUploadedOnIndex(mockRecordIndex);
        factory.setDdbRecordUploadDateIndex(mockRecordIndex);
        factory.setDdbExportTimeTable(mockExportTimeTable);
        factory.setDdbStudyTable(mockStudyTable);
        factory.setDdbClientScan(mockDdbClient);

        // execute and validate
        BridgeExporterRequest request;
        if (!withWhitelist) {
            request = new BridgeExporterRequest.Builder()
                    .withDate(UPLOAD_DATE_OBJ)
                    .withStartDateTime(UPLOAD_START_DATE_TIME_OBJ)
                    .withEndDateTime(UPLOAD_END_DATE_TIME_OBJ)
                    .withIgnoreLastExportTime(ignoreLastExportTime)
                    .withExportType(exportType)
                    .build();
        } else {
            Set<String> studyWhitelist = new LinkedHashSet<>();
            studyWhitelist.add("ddb-foo");
            studyWhitelist.add("ddb-bar");
            request = new BridgeExporterRequest.Builder()
                    .withDate(UPLOAD_DATE_OBJ)
                    .withStudyWhitelist(studyWhitelist)
                    .withStartDateTime(UPLOAD_START_DATE_TIME_OBJ)
                    .withEndDateTime(UPLOAD_END_DATE_TIME_OBJ)
                    .withIgnoreLastExportTime(ignoreLastExportTime)
                    .withExportType(exportType)
                    .build();
        }
        RecordAndStudyListHolder holder = factory.getRecordSourceForRequest(request);
        Map<String, DateTime> studyIdsToQuery = holder.getStudyIdsToQuery();
        DateTime retEndDateTime = holder.getEndDateTime();

        if (!withWhitelist) {
            verify(mockDdbClient).scan(eq(scanRequest));
        } else {
            verifyZeroInteractions(mockDdbClient);
        }

        assertEquals(studyIdsToQuery.size(), 2);

        if (withLastExportDateTime && !ignoreLastExportTime) {
            for (DateTime startDateTime : studyIdsToQuery.values()) {
                assertEquals(startDateTime.getMillis(), 123L);
            }
        } else {
            if (exportType == ExportType.DAILY) {
                for (DateTime startDateTime : studyIdsToQuery.values()) {
                    assertEquals(startDateTime, UPLOAD_START_DATE_TIME_OBJ);
                }
            } else if (exportType == ExportType.HOURLY) {
                for (DateTime startDateTime : studyIdsToQuery.values()) {
                    assertEquals(startDateTime, UPLOAD_END_DATE_TIME_OBJ.minusHours(1).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0));
                }
            } else if (exportType == ExportType.INSTANT) {
                for (DateTime startDateTime : studyIdsToQuery.values()) {
                    assertEquals(startDateTime, CURRENT_DATE_OBJ.minusDays(1));
                }
            }
        }

        if (exportType == ExportType.DAILY || exportType == ExportType.HOURLY) {
            assertEquals(retEndDateTime, UPLOAD_END_DATE_TIME_OBJ);
        } else if (exportType == ExportType.INSTANT) {
            assertEquals(retEndDateTime, CURRENT_DATE_OBJ.minusMinutes(1));
        }
    }
}
