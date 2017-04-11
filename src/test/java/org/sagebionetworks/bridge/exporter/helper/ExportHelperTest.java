package org.sagebionetworks.bridge.exporter.helper;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.LocalDate;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.record.ExportType;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.s3.S3Helper;

public class ExportHelperTest {
    private static final String DUMMY_ATTACHMENT_BUCKET = "dummy-attachment-bucket";
    private static final String DUMMY_RECORD_ID = "dummy-record-id";
    private static final String DUMMY_ATTACHMENT_CONTENT = "dummy attachment content";

    private static final String UPLOAD_DATE = "2016-05-09";
    private static final String UPLOAD_START_DATE_TIME = "2016-05-09T00:00:00.000+0900";
    private static final String UPLOAD_END_DATE_TIME = "2016-05-09T23:59:59.999+0900";

    private static final DateTime UPLOAD_START_DATE_TIME_OBJ = DateTime.parse(UPLOAD_START_DATE_TIME);
    private static final DateTime UPLOAD_END_DATE_TIME_OBJ = DateTime.parse(UPLOAD_END_DATE_TIME);
    private static final LocalDate UPLOAD_DATE_OBJ = LocalDate.parse(UPLOAD_DATE);

    @BeforeClass
    public void mockTime() {
        DateTimeUtils.setCurrentMillisFixed(UPLOAD_END_DATE_TIME_OBJ.getMillis());
    }

    @AfterClass
    public void cleanupTime() {
        DateTimeUtils.setCurrentMillisSystem();
    }

    @Test
    public void uploadFreeformText() throws Exception {
        // mock Config
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(BridgeExporterUtil.CONFIG_KEY_ATTACHMENT_S3_BUCKET)).thenReturn(DUMMY_ATTACHMENT_BUCKET);

        // set up export helper with mocks
        Table mockAttachmentsTable = mock(Table.class);
        S3Helper mockS3Helper = mock(S3Helper.class);

        ExportHelper helper = new ExportHelper();
        helper.setConfig(mockConfig);
        helper.setDdbAttachmentTable(mockAttachmentsTable);
        helper.setS3Helper(mockS3Helper);

        // execute
        String attachmentId = helper.uploadFreeformTextAsAttachment(DUMMY_RECORD_ID, DUMMY_ATTACHMENT_CONTENT);

        // Attachment ID is randomly generated. Assert that it's not blank and assert that it's the same as what we
        // write into DDB and S3.
        assertTrue(StringUtils.isNotBlank(attachmentId));

        // verify DDB attachment
        ArgumentCaptor<Item> attachmentItemCaptor = ArgumentCaptor.forClass(Item.class);
        verify(mockAttachmentsTable).putItem(attachmentItemCaptor.capture());

        Item attachmentItem = attachmentItemCaptor.getValue();
        assertEquals(attachmentItem.getString("id"), attachmentId);
        assertEquals(attachmentItem.getString("recordId"), DUMMY_RECORD_ID);

        // verify S3 attachment
        ArgumentCaptor<byte[]> attachmentBytesCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(mockS3Helper).writeBytesToS3(eq(DUMMY_ATTACHMENT_BUCKET), eq(attachmentId),
                attachmentBytesCaptor.capture());

        String attachmentText = new String(attachmentBytesCaptor.getValue(), Charsets.UTF_8);
        assertEquals(attachmentText, DUMMY_ATTACHMENT_CONTENT);
    }

    @Test
    public void getEndDateTimeTest() {
        // mock Config - Use a timezone other than PST or UTC to make sure we handle timezones correctly
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(BridgeExporterUtil.CONFIG_KEY_TIME_ZONE_NAME)).thenReturn("Asia/Tokyo");

        BridgeExporterRequest request;
        DateTime endDateTime;
        ExportHelper exportHelper = new ExportHelper();
        exportHelper.setConfig(mockConfig);

        // DAILY
        request = new BridgeExporterRequest.Builder().withEndDateTime(UPLOAD_END_DATE_TIME_OBJ).withExportType(
                ExportType.DAILY).build();
        endDateTime = exportHelper.getEndDateTime(request);

        assertEquals(endDateTime.getMillis(), UPLOAD_END_DATE_TIME_OBJ.getMillis());

        // HOURLY
        request = new BridgeExporterRequest.Builder()
                .withEndDateTime(UPLOAD_END_DATE_TIME_OBJ)
                .withExportType(ExportType.HOURLY)
                .withStudyWhitelist(ImmutableSet.of("dummy-whitelist"))
                .build();
        endDateTime = exportHelper.getEndDateTime(request);

        assertEquals(endDateTime.getMillis(), UPLOAD_END_DATE_TIME_OBJ.getMillis());

        // INSTANT
        request = new BridgeExporterRequest.Builder()
                .withExportType(ExportType.INSTANT)
                .withStudyWhitelist(ImmutableSet.of("dummy-whitelist"))
                .build();
        endDateTime = exportHelper.getEndDateTime(request);

        assertEquals(endDateTime.getMillis(), UPLOAD_END_DATE_TIME_OBJ.minusMinutes(1).getMillis());

        // s3 override
        request = new BridgeExporterRequest.Builder().withRecordIdS3Override("dummy-override").build();
        endDateTime = exportHelper.getEndDateTime(request);

        assertNull(endDateTime);
    }
}
