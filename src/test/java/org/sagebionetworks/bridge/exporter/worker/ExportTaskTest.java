package org.sagebionetworks.bridge.exporter.worker;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.PrintWriter;
import java.util.Queue;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.record.ExportType;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

public class ExportTaskTest {
    private static final LocalDate DUMMY_EXPORTER_DATE = LocalDate.parse("2015-12-07");
    private static final DateTime DUMMY_REQUEST_DATE_TIME = DateTime.parse("2015-12-07T23:59:59Z");
    private static final BridgeExporterRequest DUMMY_REQUEST = new BridgeExporterRequest.Builder()
            .withEndDateTime(DUMMY_REQUEST_DATE_TIME).withExportType(ExportType.DAILY).build();

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "exporterDate must be non-null")
    public void nullExporterDate() {
        new ExportTask.Builder().withMetrics(new Metrics()).withRequest(DUMMY_REQUEST).withTmpDir(mock(File.class))
                .build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "metrics must be non-null")
    public void nullMetrics() {
        new ExportTask.Builder().withExporterDate(DUMMY_EXPORTER_DATE).withRequest(DUMMY_REQUEST)
                .withTmpDir(mock(File.class)).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "request must be non-null")
    public void nullRequest() {
        new ExportTask.Builder().withExporterDate(DUMMY_EXPORTER_DATE).withMetrics(new Metrics())
                .withTmpDir(mock(File.class)).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "tmpDir must be non-null")
    public void nullTmpDir() {
        new ExportTask.Builder().withExporterDate(DUMMY_EXPORTER_DATE).withMetrics(new Metrics())
                .withRequest(DUMMY_REQUEST).build();
    }

    @Test
    public void happyCase() {
        // build
        Metrics metrics = new Metrics();
        File mockFile = mock(File.class);
        ExportTask task = new ExportTask.Builder().withExporterDate(DUMMY_EXPORTER_DATE).withMetrics(metrics)
                .withRequest(DUMMY_REQUEST).withTmpDir(mockFile).build();

        // validate
        assertEquals(task.getExporterDate(), DUMMY_EXPORTER_DATE);
        assertSame(task.getMetrics(), metrics);
        assertSame(task.getRequest(), DUMMY_REQUEST);
        assertSame(task.getTmpDir(), mockFile);
    }

    @Test
    public void appVersionTsvs() {
        ExportTask task = createTask();

        // set values
        TsvInfo fooTsvInfo = createTsvInfo();
        task.setAppVersionTsvInfoForStudy("foo-study", fooTsvInfo);

        TsvInfo barTsvInfo = createTsvInfo();
        task.setAppVersionTsvInfoForStudy("bar-study", barTsvInfo);

        // get values back and validate
        assertSame(task.getAppVersionTsvInfoForStudy("foo-study"), fooTsvInfo);
        assertSame(task.getAppVersionTsvInfoForStudy("bar-study"), barTsvInfo);
    }

    @Test
    public void healthDataTsvs() {
        ExportTask task = createTask();

        // set values
        UploadSchemaKey fooSchemaKey = new UploadSchemaKey.Builder().withStudyId("test-study")
                .withSchemaId("foo-schema").withRevision(3).build();
        TsvInfo fooTsvInfo = createTsvInfo();
        task.setHealthDataTsvInfoForSchema(fooSchemaKey, fooTsvInfo);

        UploadSchemaKey barSchemaKey = new UploadSchemaKey.Builder().withStudyId("test-study")
                .withSchemaId("bar-schema").withRevision(7).build();
        TsvInfo barTsvInfo = createTsvInfo();
        task.setHealthDataTsvInfoForSchema(barSchemaKey, barTsvInfo);

        // get values back and validate
        assertSame(task.getHealthDataTsvInfoForSchema(fooSchemaKey), fooTsvInfo);
        assertSame(task.getHealthDataTsvInfoForSchema(barSchemaKey), barTsvInfo);
    }

    @Test
    public void taskQueue() {
        ExportTask task = createTask();

        // add mock tasks to queue
        ExportSubtaskFuture mockFooFuture = mock(ExportSubtaskFuture.class);
        task.addSubtaskFuture(mockFooFuture);

        ExportSubtaskFuture mockBarFuture = mock(ExportSubtaskFuture.class);
        task.addSubtaskFuture(mockBarFuture);

        // get tasks back in order
        Queue<ExportSubtaskFuture> taskQueue = task.getSubtaskFutureQueue();
        assertSame(taskQueue.remove(), mockFooFuture);
        assertSame(taskQueue.remove(), mockBarFuture);
        assertTrue(taskQueue.isEmpty());
    }

    @Test
    public void studyIdSet() {
        ExportTask task = createTask();
        task.addStudyId("foo");
        task.addStudyId("bar");
        task.addStudyId("baz");

        Set<String> studyIdSet = task.getStudyIdSet();
        assertEquals(studyIdSet.size(), 3);
        assertTrue(studyIdSet.contains("foo"));
        assertTrue(studyIdSet.contains("bar"));
        assertTrue(studyIdSet.contains("baz"));
    }

    private static ExportTask createTask() {
        return new ExportTask.Builder().withExporterDate(DUMMY_EXPORTER_DATE).withMetrics(new Metrics())
                .withRequest(DUMMY_REQUEST).withTmpDir(mock(File.class)).build();
    }

    private static TsvInfo createTsvInfo() {
        // We don't actually use the TSV for anything, so it's safe to stick an empty list and 2 mocks in here.
        return new TsvInfo(ImmutableList.of(), mock(File.class), mock(PrintWriter.class));
    }
}
