package org.sagebionetworks.bridge.exporter.request;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.joda.deser.LocalDateDeserializer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;

import org.sagebionetworks.bridge.exporter.record.ExportType;
import org.sagebionetworks.bridge.json.DateTimeDeserializer;
import org.sagebionetworks.bridge.json.DateTimeToStringSerializer;
import org.sagebionetworks.bridge.json.LocalDateToStringSerializer;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

/** Encapsulates a request to Bridge EX and the ability to serialize to/from JSON. */
@JsonDeserialize(builder = BridgeExporterRequest.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BridgeExporterRequest {
    private final LocalDate date;
    private final DateTime endDateTime;
    private final String exporterDdbPrefixOverride;
    private final String recordIdS3Override;
    private final int redriveCount;
    private final BridgeExporterSharingMode sharingMode;
    private final DateTime startDateTime;
    private final Set<String> studyWhitelist;
    private final Map<String, String> synapseProjectOverrideMap;
    private final Set<UploadSchemaKey> tableWhitelist;
    private final String tag;
    private final boolean ignoreLastExportTime;
    private final ExportType exportType;

    /** Private constructor. To build, go through the builder. */
    private BridgeExporterRequest(LocalDate date, DateTime endDateTime, String exporterDdbPrefixOverride,
            String recordIdS3Override, int redriveCount, BridgeExporterSharingMode sharingMode, DateTime startDateTime,
            Set<String> studyWhitelist, Map<String, String> synapseProjectOverrideMap,
            Set<UploadSchemaKey> tableWhitelist, String tag, boolean ignoreLastExportTime, ExportType exportType) {
        this.date = date;
        this.endDateTime = endDateTime;
        this.exporterDdbPrefixOverride = exporterDdbPrefixOverride;
        this.recordIdS3Override = recordIdS3Override;
        this.redriveCount = redriveCount;
        this.sharingMode = sharingMode;
        this.startDateTime = startDateTime;
        this.studyWhitelist = studyWhitelist;
        this.synapseProjectOverrideMap = synapseProjectOverrideMap;
        this.tableWhitelist = tableWhitelist;
        this.tag = tag;
        this.ignoreLastExportTime = ignoreLastExportTime;
        this.exportType = exportType;
    }

    /**
     * Date for data that Bridge EX should export (usually yesterday's date). You must specify exactly one of date,
     * start/endDateTime, or recordIdS3Override.
     */
    @JsonSerialize(using = LocalDateToStringSerializer.class)
    public LocalDate getDate() {
        return date;
    }

    /**
     * End date, exclusive. For use with export jobs more granular than daily (such as hourly exports). See also
     * {@link #getStartDateTime}. If this is specified, so must startDateTime. You must specify exactly one of date,
     * start/endDateTime, or recordIdS3Override.
     */
    @JsonSerialize(using = DateTimeToStringSerializer.class)
    public DateTime getEndDateTime() {
        return endDateTime;
    }

    /**
     * Override for the prefix for DDB tables that keep track of Synapse tables. This is generally used for one-off
     * exports to separate Synapse projects. This is optional, but must be specified if synapseProjectOverrideMap is
     * also specified.
     */
    public String getExporterDdbPrefixOverride() {
        return exporterDdbPrefixOverride;
    }

    /**
     * Override to export a list of record IDs instead of querying DDB. This is generally used for redriving specific
     * records. You must specify exactly one of date, start/endDateTime, or recordIdS3Override.
     */
    public String getRecordIdS3Override() {
        return recordIdS3Override;
    }

    /**
     * The number of times this request has been redriven. Zero if this is the first request. 1 if this is the first
     * redrive. And so forth.
     */
    public int getRedriveCount() {
        return redriveCount;
    }

    /**
     * Whether Bridge EX should export public data, shared data, or all data. This is generally used for one-off
     * exports to non-default projects with non-default sharing settings. This is optional and defaults to "shared" if
     * left blank.
     */
    public BridgeExporterSharingMode getSharingMode() {
        return sharingMode;
    }

    /**
     * Start date, inclusive. For use with export jobs more granular than daily (such as hourly exports). See also
     * {@link #getEndDateTime}. If this is specified, so must startDateTime. You must specify exactly one of date,
     * start/endDateTime, or recordIdS3Override.
     */
    @JsonSerialize(using = DateTimeToStringSerializer.class)
    public DateTime getStartDateTime() {
        return startDateTime;
    }

    /**
     * Whitelist of studies that Bridge EX should export. This is optional and is generally used for one-off exports
     * for specific studies.
     */
    public Set<String> getStudyWhitelist() {
        return studyWhitelist;
    }

    /**
     * Override map for Synapse projects. Key is study ID. Value is Synapse project ID. This is generally used for
     * one-off exports to separate Synapse projects. This is optional, but must be specified if
     * exporterDdbPrefixOverride is also specified.
     */
    public Map<String, String> getSynapseProjectOverrideMap() {
        return synapseProjectOverrideMap;
    }

    /**
     * White list of tables (schemas) that Bridge EX should export. This is optional and is generally used for
     * redriving specific tables.
     */
    public Set<UploadSchemaKey> getTableWhitelist() {
        return tableWhitelist;
    }

    /**
     * Tag, used to trace specific requests based on their sources or based on their semantic intent. This is optional,
     * but strongly recommended.
     */
    public String getTag() {
        return tag;
    }

    public boolean getIgnoreLastExportTime() {
        return this.ignoreLastExportTime;
    }

    public ExportType getExportType() {
        return this.exportType;
    }

    @Override public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof BridgeExporterRequest)) {
            return false;
        }
        BridgeExporterRequest that = (BridgeExporterRequest) o;
        return Objects.equals(date, that.date) &&
                Objects.equals(endDateTime, that.endDateTime) &&
                Objects.equals(exporterDdbPrefixOverride, that.exporterDdbPrefixOverride) &&
                Objects.equals(recordIdS3Override, that.recordIdS3Override) &&
                redriveCount == that.redriveCount &&
                sharingMode == that.sharingMode &&
                Objects.equals(startDateTime, that.startDateTime) &&
                Objects.equals(studyWhitelist, that.studyWhitelist) &&
                Objects.equals(synapseProjectOverrideMap, that.synapseProjectOverrideMap) &&
                Objects.equals(tableWhitelist, that.tableWhitelist) &&
                Objects.equals(tag, that.tag) &&
                Objects.equals(ignoreLastExportTime, that.ignoreLastExportTime) &&
                Objects.equals(exportType, that.exportType);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(date, endDateTime, exporterDdbPrefixOverride, recordIdS3Override, redriveCount, sharingMode,
                startDateTime, studyWhitelist, synapseProjectOverrideMap, tableWhitelist, tag, ignoreLastExportTime, exportType);
    }

    /**
     * Converts the request to a string for use in log messages. Only contains the tag and a basic parameter to
     * identify the record source.
     */
    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();

        // Record source is either date, start/endDateTime, or recordIdS3Override.
        if (date != null) {
            stringBuilder.append("date=");
            stringBuilder.append(date);
        } else if ( startDateTime != null) {
            stringBuilder.append("startDateTime=");
            stringBuilder.append(startDateTime);
            stringBuilder.append(", endDateTime=");
            stringBuilder.append(endDateTime);
        } else {
            // By elimination, this must use recordIdS3Override
            stringBuilder.append("recordIdS3Override=");
            stringBuilder.append(recordIdS3Override);
        }

        // Include redriveCount, since this is helpful for logging and diagnostics.
        stringBuilder.append(", redriveCount=");
        stringBuilder.append(redriveCount);

        // Always include tag.
        stringBuilder.append(", tag=");
        stringBuilder.append(tag);

        stringBuilder.append(", ignoreLastExportTime=");
        stringBuilder.append(ignoreLastExportTime);

        stringBuilder.append(", exportType=");
        stringBuilder.append(exportType);

        return stringBuilder.toString();
    }

    /** Request builder. */
    public static class Builder {
        private LocalDate date;
        private DateTime endDateTime;
        private String exporterDdbPrefixOverride;
        private String recordIdS3Override;
        private int redriveCount;
        private BridgeExporterSharingMode sharingMode;
        private DateTime startDateTime;
        private Set<String> studyWhitelist;
        private Map<String, String> synapseProjectOverrideMap;
        private Set<UploadSchemaKey> tableWhitelist;
        private String tag;
        private boolean ignoreLastExportTime;
        private ExportType exportType;

        /** Sets the builder with a copy of the given request. */
        public Builder copyOf(BridgeExporterRequest other) {
            // Don't worry about copying collections here. This is handled by build().
            date = other.date;
            endDateTime = other.endDateTime;
            exporterDdbPrefixOverride = other.exporterDdbPrefixOverride;
            recordIdS3Override = other.recordIdS3Override;
            redriveCount = other.redriveCount;
            sharingMode = other.sharingMode;
            startDateTime = other.startDateTime;
            studyWhitelist = other.studyWhitelist;
            synapseProjectOverrideMap = other.synapseProjectOverrideMap;
            tableWhitelist = other.tableWhitelist;
            tag = other.tag;
            ignoreLastExportTime = other.ignoreLastExportTime;
            exportType = other.exportType;
            return this;
        }

        /** @see BridgeExporterRequest#getDate */
        @JsonDeserialize(using = LocalDateDeserializer.class)
        public Builder withDate(LocalDate date) {
            this.date = date;
            return this;
        }

        /** @see BridgeExporterRequest#getEndDateTime */
        @JsonDeserialize(using = DateTimeDeserializer.class)
        public Builder withEndDateTime(DateTime endDateTime) {
            this.endDateTime = endDateTime;
            return this;
        }

        /** @see BridgeExporterRequest#getExporterDdbPrefixOverride */
        public Builder withExporterDdbPrefixOverride(String exporterDdbPrefixOverride) {
            this.exporterDdbPrefixOverride = exporterDdbPrefixOverride;
            return this;
        }

        /** @see BridgeExporterRequest#getRecordIdS3Override */
        public Builder withRecordIdS3Override(String recordIdS3Override) {
            this.recordIdS3Override = recordIdS3Override;
            return this;
        }

        /** @see BridgeExporterRequest#getRedriveCount */
        public Builder withRedriveCount(int redriveCount) {
            this.redriveCount = redriveCount;
            return this;
        }

        /** @see BridgeExporterRequest#getSharingMode */
        public Builder withSharingMode(BridgeExporterSharingMode sharingMode) {
            this.sharingMode = sharingMode;
            return this;
        }

        /** @see BridgeExporterRequest#getStartDateTime */
        @JsonDeserialize(using = DateTimeDeserializer.class)
        public Builder withStartDateTime(DateTime startDateTime) {
            this.startDateTime = startDateTime;
            return this;
        }

        /** @see BridgeExporterRequest#getStudyWhitelist */
        public Builder withStudyWhitelist(Set<String> studyWhitelist) {
            this.studyWhitelist = studyWhitelist;
            return this;
        }

        /** @see BridgeExporterRequest#getSynapseProjectOverrideMap */
        public Builder withSynapseProjectOverrideMap(Map<String, String> synapseProjectOverrideMap) {
            this.synapseProjectOverrideMap = synapseProjectOverrideMap;
            return this;
        }

        /** @see BridgeExporterRequest#getTableWhitelist */
        public Builder withTableWhitelist(Set<UploadSchemaKey> tableWhitelist) {
            this.tableWhitelist = tableWhitelist;
            return this;
        }

        /** @see BridgeExporterRequest#getTag */
        public Builder withTag(String tag) {
            this.tag = tag;
            return this;
        }

        public Builder withIgnoreLastExportTime(boolean ignoreLastExportTime) {
            this.ignoreLastExportTime = ignoreLastExportTime;
            return this;
        }

        public Builder withExportType(ExportType exportType) {
            this.exportType = exportType;
            return this;
        }

        /** Builds a Bridge EX request object and validates all parameters. */
        public BridgeExporterRequest build() {
            boolean hasExportType = exportType != null;

            // startDateTime and endDateTime must be both specified or both absent.
            boolean hasStartDateTime = startDateTime != null;
            boolean hasEndDateTime = endDateTime != null;
            if (hasStartDateTime ^ hasEndDateTime) {
                throw new IllegalStateException("startDateTime and endDateTime must both be specified or both be " +
                        "absent.");
            }
            if (hasStartDateTime && !startDateTime.isBefore(endDateTime)) {
                throw new IllegalStateException("startDateTime must be before endDateTime.");
            }

            if (hasExportType) {
                if ((exportType == ExportType.DAILY || exportType == ExportType.HOURLY) && !hasEndDateTime) {
                    throw new IllegalStateException("Must specify endDateTime for daily or hourly export.");
                }
            }

            int numSources = 0;
            if (hasEndDateTime || date != null) {
                numSources++;
            }

            if (StringUtils.isNotBlank(recordIdS3Override)) {
                numSources++;
            }

            if (numSources != 1) {
                throw new IllegalStateException("Exactly one of date/start/endDateTime, and recordIdS3Override must be specified.");
            }

            // If exporterDdbPrefixOverride is specified, then so must synapseProjectOverrideMap, and vice versa.
            boolean hasExporterDdbPrefixOverride = StringUtils.isNotBlank(exporterDdbPrefixOverride);
            boolean hasSynapseProjectOverrideMap = synapseProjectOverrideMap != null;
            if (hasExporterDdbPrefixOverride ^ hasSynapseProjectOverrideMap) {
                throw new IllegalStateException("exporterDdbPrefixOverride and synapseProjectOverrideMap must both " +
                        "be specified or both be absent.");
            }

            // synapseProjectOverrideMap can be unspecified (null), but it's semantically unclear if it's an empty map.
            // Therefore, if synapseProjectOverrideMap is specified, it can't be empty.
            if (hasSynapseProjectOverrideMap && synapseProjectOverrideMap.isEmpty()) {
                throw new IllegalStateException("If synapseProjectOverrideMap is specified, it can't be empty.");
            }

            // Similarly, studyWhitelist and tableWhitelist can be unspecified, but can't be empty.
            if (studyWhitelist != null && studyWhitelist.isEmpty()) {
                throw new IllegalStateException("If studyWhitelist is specified, it can't be empty.");
            }

            if (tableWhitelist != null && tableWhitelist.isEmpty()) {
                throw new IllegalStateException("If tableWhitelist is specified, it can't be empty.");
            }

            // sharingMode defaults to SHARED if not specified
            if (sharingMode == null) {
                sharingMode = BridgeExporterSharingMode.SHARED;
            }

            // tag is always optional and doesn't need to be validated

            // Replace collections with immutable copies, to maintain request object integrity.
            if (studyWhitelist != null) {
                studyWhitelist = ImmutableSet.copyOf(studyWhitelist);
            }

            if (hasSynapseProjectOverrideMap) {
                synapseProjectOverrideMap = ImmutableMap.copyOf(synapseProjectOverrideMap);
            }

            if (tableWhitelist != null) {
                tableWhitelist = ImmutableSet.copyOf(tableWhitelist);
            }

            return new BridgeExporterRequest(date, endDateTime, exporterDdbPrefixOverride, recordIdS3Override,
                    redriveCount, sharingMode, startDateTime, studyWhitelist, synapseProjectOverrideMap,
                    tableWhitelist, tag, ignoreLastExportTime, exportType);
        }
    }
}
