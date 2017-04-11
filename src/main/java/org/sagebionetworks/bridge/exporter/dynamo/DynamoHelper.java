package org.sagebionetworks.bridge.exporter.dynamo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Resource;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.collect.ImmutableMap;
import com.jcabi.aspects.Cacheable;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.dynamodb.DynamoScanHelper;
import org.sagebionetworks.bridge.exporter.record.ExportType;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;

/**
 * Encapsulates common calls Bridge-EX makes to DDB. Some of these have a little bit of logic or require marshalling
 * from a DDB item to a Bridge-EX object. Others are there just for convenience, so we have all or most of our DDB
 * interactions through a single class.
 */
@Component
public class DynamoHelper {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoHelper.class);

    private static final String STUDY_INFO_KEY_DATA_ACCESS_TEAM = "synapseDataAccessTeamId";
    private static final String STUDY_INFO_KEY_PROJECT_ID = "synapseProjectId";
    private static final String STUDY_INFO_KEY_USES_CUSTOM_EXPORT_SCHEDULE = "usesCustomExportSchedule";
    static final String STUDY_DISABLE_EXPORT = "disableExport";

    static final String IDENTIFIER = "identifier";
    static final String LAST_EXPORT_DATE_TIME = "lastExportDateTime";
    static final String STUDY_ID = "studyId";

    private Table ddbParticipantOptionsTable;
    private Table ddbStudyTable;
    private Table ddbExportTimeTable;
    private DynamoScanHelper ddbScanHelper;
    private DateTimeZone timeZone;

    /** Config, used to get S3 bucket for record ID override files. */
    @Autowired
    final void setConfig(Config config) {
        timeZone = DateTimeZone.forID(config.get(BridgeExporterUtil.CONFIG_KEY_TIME_ZONE_NAME));
    }

    /** Participant options table, used to get user sharing scope. */
    @Resource(name = "ddbParticipantOptionsTable")
    public final void setDdbParticipantOptionsTable(Table ddbParticipantOptionsTable) {
        this.ddbParticipantOptionsTable = ddbParticipantOptionsTable;
    }

    /** Study table, used to get study config, like linked Synapse project. */
    @Resource(name = "ddbStudyTable")
    public final void setDdbStudyTable(Table ddbStudyTable) {
        this.ddbStudyTable = ddbStudyTable;
    }

    /** DDB Export Time Table. */
    @Resource(name = "ddbExportTimeTable")
    final void setDdbExportTimeTable(Table ddbExportTimeTable) {
        this.ddbExportTimeTable = ddbExportTimeTable;
    }

    @Autowired
    final void setDdbScanHelper(DynamoScanHelper ddbScanHelper) {
        this.ddbScanHelper = ddbScanHelper;
    }

    /**
     * Gets the sharing scope for the given user.
     *
     * @param healthCode
     *         health code of user to get sharing scope for
     * @return the user's sharing scope
     */
    @Cacheable(lifetime = 5, unit = TimeUnit.MINUTES)
    public SharingScope getSharingScopeForUser(String healthCode) {
        // default sharing scope is no sharing
        SharingScope sharingScope = SharingScope.NO_SHARING;

        try {
            Item participantOptionsItem = ddbParticipantOptionsTable.getItem("healthDataCode", healthCode);
            if (participantOptionsItem != null) {
                String participantOptionsData = participantOptionsItem.getString("data");
                Map<String, Object> participantOptionsDataMap = DefaultObjectMapper.INSTANCE.readValue(
                        participantOptionsData, DefaultObjectMapper.TYPE_REF_RAW_MAP);
                String sharingScopeStr = String.valueOf(participantOptionsDataMap.get("SHARING_SCOPE"));

                // put this in its own try-catch block for better logging
                try {
                    sharingScope = SharingScope.valueOf(sharingScopeStr);
                } catch (IllegalArgumentException ex) {
                    LOG.error("Unable to parse sharing options for hash[healthCode]=" + healthCode.hashCode() +
                            ", sharing scope value=" + sharingScopeStr);
                }
            }
        } catch (IOException | RuntimeException ex) {
            // log an error, fall back to default
            LOG.error("Unable to get sharing options for hash[healthCode]=" + healthCode.hashCode() + ": " +
                    ex.getMessage(), ex);
        }

        return sharingScope;
    }

    /**
     * Get study info, namely Synapse project and data access team.
     *
     * @param studyId
     *         study ID to fetch
     * @return study info
     */
    @Cacheable(lifetime = 5, unit = TimeUnit.MINUTES)
    public StudyInfo getStudyInfo(String studyId) {
        Item studyItem = ddbStudyTable.getItem("identifier", studyId);

        // DDB's Item.getLong() will throw if the value is null. For robustness, check get() the value and check that
        // it's not null. (This should cover both cases where the attribute doesn't exist and cases where the attribute
        // exists and is null.
        StudyInfo.Builder studyInfoBuilder = new StudyInfo.Builder();

        // we need to check at first if the study is disabled exporting or not
        // treat null as false

        if (studyItem.get(STUDY_INFO_KEY_DATA_ACCESS_TEAM) != null) {
            studyInfoBuilder.withDataAccessTeamId(studyItem.getLong(STUDY_INFO_KEY_DATA_ACCESS_TEAM));
        }

        if (studyItem.get(STUDY_INFO_KEY_PROJECT_ID) != null) {
            studyInfoBuilder.withSynapseProjectId(studyItem.getString(STUDY_INFO_KEY_PROJECT_ID));
        }

        if (studyItem.get(STUDY_INFO_KEY_USES_CUSTOM_EXPORT_SCHEDULE) != null) {
            // For some reason, the mapper saves the value as an int, not as a boolean.
            boolean usesCustomExportSchedule = parseDdbBoolean(studyItem, STUDY_INFO_KEY_USES_CUSTOM_EXPORT_SCHEDULE);
            studyInfoBuilder.withUsesCustomExportSchedule(usesCustomExportSchedule);
        }

        if (studyItem.get(STUDY_DISABLE_EXPORT) != null) {
            boolean disableExport = parseDdbBoolean(studyItem, STUDY_DISABLE_EXPORT);
            studyInfoBuilder.withDisableExport(disableExport);
        }

        return studyInfoBuilder.build();
    }

    /**
     * Helper method to generate study ids for query
     * @param request
     * @return
     */
    public List<String> bootstrapStudyIdsToQuery(BridgeExporterRequest request, DateTime endDateTime) {
        List<String> studyIdList = new ArrayList<>();

        if (request.getStudyWhitelist() == null) {
            // get the study id list from ddb table
            Iterable<Item> scanOutcomes = ddbScanHelper.scan(ddbStudyTable);
            for (Item item: scanOutcomes) {
                studyIdList.add(item.getString(IDENTIFIER));
            }
        } else {
            studyIdList.addAll(request.getStudyWhitelist());
        }

        List<String> studyIdsToQuery = new ArrayList<>();

        for (String studyId : studyIdList) {
            Item studyIdItem = ddbExportTimeTable.getItem(STUDY_ID, studyId);
            if (studyIdItem != null) {
                DateTime lastExportDateTime = new DateTime(studyIdItem.getLong(LAST_EXPORT_DATE_TIME), timeZone);
                if (!endDateTime.isBefore(lastExportDateTime)) {
                    studyIdsToQuery.add(studyId);
                }
            } else {
                studyIdsToQuery.add(studyId);
            }

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                LOG.error("Unable to sleep thread: " +
                        e.getMessage(), e);
            }
        }

        return studyIdsToQuery;
    }

    /**
     * Helper method to generate study ids for query
     * @param request
     * @return
     */
    public Map<String, DateTime> bootstrapStudyIdsToQuery(BridgeExporterRequest request, DateTime endDateTime) {
        // first check if it is s3 override request
        if (StringUtils.isNotBlank(request.getRecordIdS3Override())) {
            return ImmutableMap.of();
        }

        List<String> studyIdList = new ArrayList<>();

        if (request.getStudyWhitelist() == null) {
            // get the study id list from ddb table
            Iterable<Item> scanOutcomes = ddbScanHelper.scan(ddbStudyTable);
            for (Item item: scanOutcomes) {
                studyIdList.add(item.getString(IDENTIFIER));
            }
        } else {
            studyIdList.addAll(request.getStudyWhitelist());
        }

        // maintain insert order for testing
        Map<String, DateTime> studyIdsToQuery = new HashMap<>();

        for (String studyId : studyIdList) {
            Item studyIdItem = ddbExportTimeTable.getItem(STUDY_ID, studyId);
            if (studyIdItem != null && !request.getIgnoreLastExportTime()) {
                DateTime lastExportDateTime = new DateTime(studyIdItem.getLong(LAST_EXPORT_DATE_TIME), timeZone);
                if (!endDateTime.isBefore(lastExportDateTime)) {
                    studyIdsToQuery.put(studyId, lastExportDateTime);
                }
            } else {
                // bootstrap startDateTime with the exportType in request
                ExportType exportType = request.getExportType();
                studyIdsToQuery.put(studyId, exportType.getStartDateTime(endDateTime));
            }

            // then sleep 1 sec before next read
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                LOG.error("Unable to sleep thread: " +
                        e.getMessage(), e);
            }
        }

        return studyIdsToQuery;
    }

    /**
     * Helper method to update ddb exportTimeTable
     * @param studyIdsToUpdate
     * @param endDateTime
     */
    public void updateExportTimeTable(List<String> studyIdsToUpdate, DateTime endDateTime) {
        if (!studyIdsToUpdate.isEmpty() && endDateTime != null) {
            for (String studyId: studyIdsToUpdate) {
                try {
                    ddbExportTimeTable.putItem(new Item().withPrimaryKey(STUDY_ID, studyId).withNumber(LAST_EXPORT_DATE_TIME, endDateTime.getMillis()));
                } catch (RuntimeException ex) {
                    LOG.error("Unable to update export time table for study id: " + studyId +
                            ex.getMessage(), ex);
                }

                // sleep 1 sec before next write
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    LOG.error("Unable to sleep thread: " +
                            e.getMessage(), e);
                }
            }
        }
    }

    /**
     * Helper function to parse ddb boolean value into boolean
     * @return
     */
    private boolean parseDdbBoolean(Item item, String attributeName) {
        return item.getInt(attributeName) != 0;
    }
}
