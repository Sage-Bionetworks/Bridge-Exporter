package org.sagebionetworks.bridge.exporter;

public enum ExportMode {
    /** Normal export - export all records for a given date */
    EXPORT,

    /** Redrive a list of tables - export all records for the table list for a given date */
    REDRIVE_TABLES,

    /** Redrive a list of records - redrive all records specified. Can be across multiple dates */
    REDRIVE_RECORDS,
}
