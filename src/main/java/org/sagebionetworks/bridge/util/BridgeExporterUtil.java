package org.sagebionetworks.bridge.util;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import org.sagebionetworks.bridge.exporter.UploadSchemaKey;

public class BridgeExporterUtil {
    public static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    // TODO: make timezone configurable
    public static final DateTimeZone LOCAL_TIME_ZONE = DateTimeZone.forID("America/Los_Angeles");

    // app versions look like "version 1.0, build 7", where "build 7" can be anything. Anything that
    // starts with "version 1.0," is v1, so we need to filter that out.
    public static final String V1_PREFIX = "version 1.0,";

    public static String getCurrentLocalTimestamp() {
        return DateTime.now(LOCAL_TIME_ZONE).toString(ISODateTimeFormat.dateTime());
    }

    public static boolean shouldConvertFreeformTextToAttachment(UploadSchemaKey schemaKey, String fieldName) {
        // When we initially designed these schemas, we didn't realize Synapse had a character limit on strings.
        // These strings may exceed that character limit, so we need this special hack to convert these strings to
        // attachments. This code applies only to legacy schemas. New schemas need to declare ATTACHMENT_BLOB,
        // otherwise the strings get automatically truncated.

        if ("breastcancer".equals(schemaKey.getStudyId())) {
            if ("BreastCancer-DailyJournal".equals(schemaKey.getSchemaId())) {
                if (schemaKey.getRev() == 1) {
                    return "content_data.APHMoodLogNoteText".equals(fieldName)
                            || "DailyJournalStep103_data.content".equals(fieldName);
                }
            } else if ("BreastCancer-ExerciseSurvey".equals(schemaKey.getSchemaId())) {
                if (schemaKey.getRev() == 1) {
                    return "exercisesurvey101_data.result".equals(fieldName)
                            || "exercisesurvey102_data.result".equals(fieldName)
                            || "exercisesurvey103_data.result".equals(fieldName)
                            || "exercisesurvey104_data.result".equals(fieldName)
                            || "exercisesurvey105_data.result".equals(fieldName)
                            || "exercisesurvey106_data.result".equals(fieldName);
                }
            }
        }

        return false;
    }

    public static String getDdbStringRemoveTabsAndTrim(Item ddbItem, String key, int maxLength, String recordId) {
        return trimToLengthAndWarn(removeTabs(ddbItem.getString(key)), maxLength, recordId);
    }

    public static String getJsonStringRemoveTabsAndTrim(JsonNode node, String key, int maxLength, String recordId) {
        return trimToLengthAndWarn(removeTabs(getJsonString(node, key)), maxLength, recordId);
    }

    private static String getJsonString(JsonNode node, String key) {
        if (node.hasNonNull(key)) {
            return node.get(key).textValue();
        } else {
            return null;
        }
    }

    public static String removeTabs(String in) {
        if (in != null) {
            // also strip newlines and carriage returns
            return in.replaceAll("[\n\r\t]+", " ");
        } else {
            return null;
        }
    }

    public static String trimToLengthAndWarn(String in, int maxLength, String recordId) {
        if (in != null && in.length() > maxLength) {
            System.out.println("[ERROR] Truncating string " + in + " to length " + maxLength + " for record "
                    + recordId);
            return in.substring(0, maxLength);
        } else {
            return in;
        }
    }
}
