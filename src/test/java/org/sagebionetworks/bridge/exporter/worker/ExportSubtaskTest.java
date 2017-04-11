package org.sagebionetworks.bridge.exporter.worker;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import java.io.File;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.record.ExportType;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

public class ExportSubtaskTest {
    private static final DateTime DUMMY_REQUEST_DATE_TIME = DateTime.parse("2015-12-06T23:59:59Z");
    private static final BridgeExporterRequest DUMMY_REQUEST = new BridgeExporterRequest.Builder()
            .withEndDateTime(DUMMY_REQUEST_DATE_TIME).withExportType(ExportType.DAILY).build();
    private static final ExportTask DUMMY_PARENT_TASK = new ExportTask.Builder()
            .withExporterDate(LocalDate.parse("2015-12-07")).withMetrics(new Metrics()).withRequest(DUMMY_REQUEST)
            .withTmpDir(mock(File.class)).build();

    private static final JsonNode DUMMY_RECORD_DATA = DefaultObjectMapper.INSTANCE.createObjectNode();
    private static final UploadSchemaKey DUMMY_SCHEMA_KEY = new UploadSchemaKey.Builder().withStudyId("test-study")
            .withSchemaId("test-schema").withRevision(17).build();

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "originalRecord must be non-null")
    public void nullOriginalRecord() {
        new ExportSubtask.Builder().withParentTask(DUMMY_PARENT_TASK).withRecordData(DUMMY_RECORD_DATA)
                .withSchemaKey(DUMMY_SCHEMA_KEY).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "parentTask must be non-null")
    public void nullParentTask() {
        new ExportSubtask.Builder().withOriginalRecord(new Item()).withRecordData(DUMMY_RECORD_DATA)
                .withSchemaKey(DUMMY_SCHEMA_KEY).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "recordData must be non-null")
    public void nullRecordData() {
        new ExportSubtask.Builder().withOriginalRecord(new Item()).withParentTask(DUMMY_PARENT_TASK)
                .withSchemaKey(DUMMY_SCHEMA_KEY).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "schemaKey must be non-null")
    public void nullSchemaKey() {
        new ExportSubtask.Builder().withOriginalRecord(new Item()).withParentTask(DUMMY_PARENT_TASK)
                .withRecordData(DUMMY_RECORD_DATA).build();
    }

    @Test
    public void happyCase() {
        // build
        Item originalRecord = new Item().withString(ExportSubtask.KEY_RECORD_ID, "dummy-id");
        ExportSubtask subtask = new ExportSubtask.Builder().withOriginalRecord(originalRecord)
                .withParentTask(DUMMY_PARENT_TASK).withRecordData(DUMMY_RECORD_DATA).withSchemaKey(DUMMY_SCHEMA_KEY)
                .build();

        // validate
        assertSame(subtask.getOriginalRecord(), originalRecord);
        assertSame(subtask.getParentTask(), DUMMY_PARENT_TASK);
        assertSame(subtask.getRecordData(), DUMMY_RECORD_DATA);
        assertEquals(subtask.getRecordId(), "dummy-id");
        assertEquals(subtask.getSchemaKey(), DUMMY_SCHEMA_KEY);
    }
}
