package org.sagebionetworks.bridge.exporter.record;

import static org.testng.Assert.assertEquals;

import org.joda.time.DateTime;
import org.testng.annotations.Test;

public class ExportTypeTest {
    private static final String END_DATE_TIME_STR = "2016-05-09T23:59:59.999-0700";
    private static final DateTime END_DATE_TIME = DateTime.parse(END_DATE_TIME_STR);

    @Test
    public void testGetStartDateTime() {
        ExportType testExportType = ExportType.DAILY;
        DateTime startDateTime = testExportType.getStartDateTime(END_DATE_TIME);
        assertEquals(startDateTime, END_DATE_TIME.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0));

        testExportType = ExportType.HOURLY;
        startDateTime = testExportType.getStartDateTime(END_DATE_TIME);
        assertEquals(startDateTime, END_DATE_TIME.minusHours(1).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0));

        testExportType = ExportType.INSTANT;
        startDateTime = testExportType.getStartDateTime(END_DATE_TIME);
        assertEquals(startDateTime, END_DATE_TIME.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0));
    }
}
