package org.sagebionetworks.bridge.exporter.record;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.sagebionetworks.bridge.exporter.record.RecordIdSourceFactory.STUDY_ID;
import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.KeyConditions;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.dynamodb.DynamoQueryHelper;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.s3.S3Helper;

public class RecordIdSourceFactoryTest {
    private static final String UPLOAD_START_DATE_TIME = "2015-11-11T00:00:00Z";
    private static final String UPLOAD_END_DATE_TIME = "2015-11-11T23:59:59Z";

    private static final DateTime UPLOAD_START_DATE_TIME_OBJ = DateTime.parse(UPLOAD_START_DATE_TIME);
    private static final DateTime UPLOAD_END_DATE_TIME_OBJ = DateTime.parse(UPLOAD_END_DATE_TIME);

    @Test
    public void fromDdbNormal() throws Exception {
        fromDdb(false);
    }

    @Test
    public void fromDdbEndDateTimeBeforeLastExportTime() throws Exception {
        fromDdb(true);
    }

    private static void fromDdb(boolean isEndDateTimeBeforeLastExportTime) throws Exception {
        // mock map
        Map<String, DateTime> studyIdsToQuery;
        if (!isEndDateTimeBeforeLastExportTime) {
            studyIdsToQuery = ImmutableMap.of("ddb-foo", UPLOAD_START_DATE_TIME_OBJ, "ddb-bar", UPLOAD_START_DATE_TIME_OBJ);
        } else {
            studyIdsToQuery = ImmutableMap.of("ddb-foo", UPLOAD_END_DATE_TIME_OBJ, "ddb-bar", UPLOAD_END_DATE_TIME_OBJ);
        }

        Index mockRecordIndex = mock(Index.class);
        DynamoQueryHelper mockQueryHelper = mock(DynamoQueryHelper.class);

        ArgumentCaptor<RangeKeyCondition> barRangeKeyCaptor = ArgumentCaptor.forClass(RangeKeyCondition.class);
        ArgumentCaptor<RangeKeyCondition> fooRangeKeyCaptor = ArgumentCaptor.forClass(RangeKeyCondition.class);

        // mock DDB
        List<Item> fooStudyItemList = ImmutableList.of(new Item().withString("id", "foo-1"),
                new Item().withString("id", "foo-2"));
        List<Item> barStudyItemList = ImmutableList.of(new Item().withString("id", "bar-1"),
                new Item().withString("id", "bar-2"));

        when(mockQueryHelper.query(same(mockRecordIndex), eq(STUDY_ID), eq("ddb-bar"), barRangeKeyCaptor.capture()))
                .thenReturn(barStudyItemList);

        when(mockQueryHelper.query(same(mockRecordIndex), eq(STUDY_ID), eq("ddb-foo"), fooRangeKeyCaptor.capture()))
                .thenReturn(fooStudyItemList);

        // set up factory
        RecordIdSourceFactory factory = new RecordIdSourceFactory();
        factory.setDdbQueryHelper(mockQueryHelper);
        factory.setConfig(mockConfig());
        factory.setDdbRecordStudyUploadedOnIndex(mockRecordIndex);

        // execute and validate
        BridgeExporterRequest request;
        request = new BridgeExporterRequest.Builder()
                .withEndDateTime(UPLOAD_END_DATE_TIME_OBJ)
                .withExportType(ExportType.DAILY)
                .build();

        Iterable<String> recordIdIter = factory.getRecordSourceForRequest(request, UPLOAD_END_DATE_TIME_OBJ, studyIdsToQuery);

        List<String> recordIdList = ImmutableList.copyOf(recordIdIter);

        if (!isEndDateTimeBeforeLastExportTime) {
            assertEquals(recordIdList.size(), 4); // only output records in given time range
            assertEquals(recordIdList.get(0), "foo-1");
            assertEquals(recordIdList.get(1), "foo-2");
            assertEquals(recordIdList.get(2), "bar-1");
            assertEquals(recordIdList.get(3), "bar-2");

            validateRangeKey(fooRangeKeyCaptor.getValue(), UPLOAD_START_DATE_TIME_OBJ.getMillis(), UPLOAD_END_DATE_TIME_OBJ.getMillis());
            validateRangeKey(barRangeKeyCaptor.getValue(), UPLOAD_START_DATE_TIME_OBJ.getMillis(), UPLOAD_END_DATE_TIME_OBJ.getMillis());
        } else {
            assertEquals(recordIdList.size(), 4); // only output records in given time range
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
        Iterable<String> recordIdIter = factory.getRecordSourceForRequest(request, UPLOAD_END_DATE_TIME_OBJ, ImmutableMap.of());

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
