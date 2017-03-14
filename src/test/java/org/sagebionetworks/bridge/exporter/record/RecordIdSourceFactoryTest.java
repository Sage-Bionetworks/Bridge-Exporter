package org.sagebionetworks.bridge.exporter.record;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
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
    private static final String STUDY_TABLE_NAME = "Study";

    private static final DateTime UPLOAD_START_DATE_TIME_OBJ = DateTime.parse(UPLOAD_START_DATE_TIME);
    private static final DateTime UPLOAD_END_DATE_TIME_OBJ = DateTime.parse(UPLOAD_END_DATE_TIME);

    @Test
    public void fromDdbWithLastExportDateTime() throws Exception {
        fromDdb(false, true, false);
    }

    @Test
    public void fromDdbWithoutLastExportDateTime() throws Exception {
        fromDdb(false, false, false);
    }

    @Test
    public void fromDdbWithReExport() throws Exception {
        // doesn't care about if it has last export date time for re export
        fromDdb(true, true, false);
        fromDdb(true, false, false);
    }

    @Test
    public void fromDdbWithWhitelistWithLastExportDateTime() throws Exception {
        fromDdb(false, true, true);
    }

    @Test
    public void fromDdbWithWhitelistWithoutLastExportDateTime() throws Exception {
        fromDdb(false, false, true);
    }

    @Test
    public void fromDdbWithWhitelistWithReExport() throws Exception {
        fromDdb(true, true, true);
        fromDdb(true, false, true);
    }

    private static void fromDdb(boolean isReExport, boolean withLastExportDateTime, boolean withWhitelist) throws Exception{
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

        ArgumentCaptor<RangeKeyCondition> fooRangeKeyCaptor = ArgumentCaptor.forClass(RangeKeyCondition.class);
        when(mockQueryHelper.query(same(mockRecordIndex), eq(STUDY_ID), eq("ddb-foo"), fooRangeKeyCaptor.capture()))
                .thenReturn(fooStudyItemList);

        // set up factory
        RecordIdSourceFactory factory = new RecordIdSourceFactory();
        factory.setDdbQueryHelper(mockQueryHelper);
        factory.setConfig(mockConfig());
        factory.setDdbRecordStudyUploadedOnIndex(mockRecordIndex);
        factory.setDdbExportTimeTable(mockExportTimeTable);
        factory.setDdbStudyTable(mockStudyTable);
        factory.setDdbClientScan(mockDdbClient);

        // execute and validate
        BridgeExporterRequest request;
        if (!withWhitelist) {
            request = new BridgeExporterRequest.Builder().withDate(LocalDate.parse(UPLOAD_DATE))
                    .withStartDateTime(UPLOAD_START_DATE_TIME_OBJ)
                    .withEndDateTime(UPLOAD_END_DATE_TIME_OBJ)
                    .withReExport(isReExport)
                    .build();
        } else {
            Set<String> studyWhitelist = new LinkedHashSet<>();
            studyWhitelist.add("ddb-foo");
            studyWhitelist.add("ddb-bar");
            request = new BridgeExporterRequest.Builder().withDate(LocalDate.parse(UPLOAD_DATE))
                    .withStudyWhitelist(studyWhitelist)
                    .withStartDateTime(UPLOAD_START_DATE_TIME_OBJ)
                    .withEndDateTime(UPLOAD_END_DATE_TIME_OBJ)
                    .withReExport(isReExport)
                    .build();
        }
        Iterable<String> recordIdIter = factory.getRecordSourceForRequest(request);

        List<String> recordIdList = ImmutableList.copyOf(recordIdIter);
        assertEquals(recordIdList.size(), 4); // only output records in given time range
        assertEquals(recordIdList.get(0), "foo-1");
        assertEquals(recordIdList.get(1), "foo-2");
        assertEquals(recordIdList.get(2), "bar-1");
        assertEquals(recordIdList.get(3), "bar-2");

        if (!withWhitelist) {
            verify(mockDdbClient).scan(eq(scanRequest));
        } else {
            verifyZeroInteractions(mockDdbClient);
        }

        if (withLastExportDateTime && !isReExport) {
            validateRangeKey(fooRangeKeyCaptor.getValue(), 123L, UPLOAD_END_DATE_TIME_OBJ.getMillis() - 1);
            validateRangeKey(barRangeKeyCaptor.getValue(), 123L, UPLOAD_END_DATE_TIME_OBJ.getMillis() - 1);
        } else {
            validateRangeKey(fooRangeKeyCaptor.getValue(), UPLOAD_START_DATE_TIME_OBJ.getMillis(), UPLOAD_END_DATE_TIME_OBJ.getMillis() - 1);
            validateRangeKey(barRangeKeyCaptor.getValue(), UPLOAD_START_DATE_TIME_OBJ.getMillis(), UPLOAD_END_DATE_TIME_OBJ.getMillis() - 1);
        }

        if (!isReExport) {
            verify(mockExportTimeTable, times(2)).putItem(any(Item.class));
        } else {
            verify(mockExportTimeTable, times(0)).putItem(any(Item.class));
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
