package org.sagebionetworks.bridge.exporter.record;

import java.util.Map;

import org.joda.time.DateTime;

public class RecordAndStudyListHolder {
    private Iterable<String> recordIdSource;
    private Map<String, DateTime> studyIdsToQuery;
    private DateTime endDateTime;

    public RecordAndStudyListHolder(Iterable<String> recordIdSource, Map<String, DateTime> studyIdsToQuery, DateTime endDateTime) {
        this.recordIdSource = recordIdSource;
        this.studyIdsToQuery = studyIdsToQuery;
        this.endDateTime = endDateTime;
    }

    public Iterable<String> getRecordIdSource() {
        return recordIdSource;
    }

    public void setRecordIdSource(Iterable<String> recordIdSource) {
        this.recordIdSource = recordIdSource;
    }

    public Map<String, DateTime> getStudyIdsToQuery() {
        return studyIdsToQuery;
    }

    public void setStudyIdsToQuery(Map<String, DateTime> studyIdsToQuery) {
        this.studyIdsToQuery = studyIdsToQuery;
    }

    public DateTime getEndDateTime() {
        return endDateTime;
    }

    public void setEndDateTime(DateTime endDateTime) {
        this.endDateTime = endDateTime;
    }
}
