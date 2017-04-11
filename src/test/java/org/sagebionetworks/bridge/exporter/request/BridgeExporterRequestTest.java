package org.sagebionetworks.bridge.exporter.request;

import static org.sagebionetworks.bridge.exporter.record.ExportType.DAILY;
import static org.sagebionetworks.bridge.exporter.record.ExportType.HOURLY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exporter.record.ExportType;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

public class BridgeExporterRequestTest {
    private static final String END_DATE_TIME_STRING = "2016-05-09T13:53:13.801-0700";
    private static final DateTime END_DATE_TIME = DateTime.parse(END_DATE_TIME_STRING);

    private static final String START_DATE_TIME_STRING = "2016-05-09T13:51:57.682-0700";
    private static final DateTime START_DATE_TIME = DateTime.parse(START_DATE_TIME_STRING);

    private static final Set<String> STUDY_WHITELIST = ImmutableSet.of("test-study");

    private static final String TEST_DATE_STRING = "2015-11-30";
    private static final LocalDate TEST_DATE = LocalDate.parse(TEST_DATE_STRING);

    private static final String TEST_DDB_PREFIX_OVERRIDE = "test-ddb-prefix-override";
    private static final Map<String, String> TEST_PROJECT_OVERRIDE_MAP = ImmutableMap.of("test-study",
            "test-project-id");
    private static final String TEST_RECORD_OVERRIDE = "test-record-override";
    private static final UploadSchemaKey TEST_SCHEMA_KEY = new UploadSchemaKey.Builder().withStudyId("test-study")
            .withSchemaId("test-schema").withRevision(13).build();
    private static final String TEST_TAG = "test-tag";

    @Test
    public void withEndDateTime() {
        BridgeExporterRequest request = new BridgeExporterRequest.Builder()
                .withEndDateTime(END_DATE_TIME).withExportType(DAILY).withStudyWhitelist(STUDY_WHITELIST).build();
        assertEquals(request.getEndDateTime(), END_DATE_TIME);
        assertEquals(request.getSharingMode(), BridgeExporterSharingMode.SHARED);
        assertEquals(request.getStudyWhitelist(), STUDY_WHITELIST);

        // test toString
        assertEquals(request.toString(), "endDateTime=" + END_DATE_TIME
                + ", redriveCount=0, tag=null, ignoreLastExportTime=false, exportType=DAILY");

        // test copy
        BridgeExporterRequest copy = new BridgeExporterRequest.Builder().copyOf(request).build();
        assertEquals(copy, request);
    }

    @Test
    public void withRecordOverride() {
        BridgeExporterRequest request = new BridgeExporterRequest.Builder()
                .withRecordIdS3Override(TEST_RECORD_OVERRIDE).build();
        assertEquals(request.getRecordIdS3Override(), TEST_RECORD_OVERRIDE);
        assertEquals(request.getSharingMode(), BridgeExporterSharingMode.SHARED);

        // test toString
        assertEquals(request.toString(), "recordIdS3Override=" + TEST_RECORD_OVERRIDE + ", redriveCount=0, tag=null, ignoreLastExportTime=false, exportType=null");

        // test copy
        BridgeExporterRequest copy = new BridgeExporterRequest.Builder().copyOf(request).build();
        assertEquals(copy, request);
    }

    @Test
    public void withOptionalParams() {
        // Make collections. We make them specifically for this test, because we want to modify them to make sure they
        // we can't backdoor-modify the request.
        Set<String> originalStudyWhitelist = Sets.newHashSet("foo-study", "bar-study");

        Map<String, String> originalProjectOverrideMap = new HashMap<>();
        originalProjectOverrideMap.put("foo-study", "foo-project-id");
        originalProjectOverrideMap.put("bar-study", "bar-project-id");

        UploadSchemaKey fooSchemaKey = new UploadSchemaKey.Builder().withStudyId("foo-study")
                .withSchemaId("foo-schema").withRevision(3).build();
        UploadSchemaKey barSchemaKey = new UploadSchemaKey.Builder().withStudyId("bar-study")
                .withSchemaId("bar-schema").withRevision(7).build();
        Set<UploadSchemaKey> originalTableWhitelist = Sets.newHashSet(fooSchemaKey, barSchemaKey);

        // make request
        BridgeExporterRequest request = new BridgeExporterRequest.Builder().withEndDateTime(END_DATE_TIME)
                .withExportType(DAILY)
                .withExporterDdbPrefixOverride(TEST_DDB_PREFIX_OVERRIDE).withRedriveCount(1)
                .withSharingMode(BridgeExporterSharingMode.PUBLIC_ONLY).withStudyWhitelist(originalStudyWhitelist)
                .withSynapseProjectOverrideMap(originalProjectOverrideMap).withTableWhitelist(originalTableWhitelist)
                .withTag(TEST_TAG).build();

        // validate
        assertEquals(request.getExporterDdbPrefixOverride(), TEST_DDB_PREFIX_OVERRIDE);
        assertNull(request.getRecordIdS3Override());
        assertEquals(request.getRedriveCount(), 1);
        assertEquals(request.getSharingMode(), BridgeExporterSharingMode.PUBLIC_ONLY);
        assertEquals(request.getStudyWhitelist(), originalStudyWhitelist);
        assertEquals(request.getSynapseProjectOverrideMap(), originalProjectOverrideMap);
        assertEquals(request.getTableWhitelist(), originalTableWhitelist);
        assertEquals(request.getTag(), TEST_TAG);

        // Validate that changes to the original collections won't be reflected in the request.
        originalStudyWhitelist.add("new-study");
        assertFalse(request.getStudyWhitelist().contains("new-study"));

        originalProjectOverrideMap.put("new-study", "new-project-id");
        assertFalse(request.getSynapseProjectOverrideMap().containsKey("new-study"));

        originalTableWhitelist.add(TEST_SCHEMA_KEY);
        assertFalse(request.getTableWhitelist().contains(TEST_SCHEMA_KEY));

        // test toString
        assertEquals(request.toString(), "endDateTime=2016-05-09T13:53:13.801-07:00, redriveCount=1, tag=" + TEST_TAG + ", ignoreLastExportTime=false, exportType=DAILY");

        // test copy
        BridgeExporterRequest copy = new BridgeExporterRequest.Builder().copyOf(request).build();
        assertEquals(copy, request);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "Must specify endDateTime for daily or hourly export.")
    public void dailyWithNoEndDateTIme() {
        new BridgeExporterRequest.Builder().withExportType(DAILY).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "Must specify endDateTime for daily or hourly export.")
    public void houlyWithNoEndDateTIme() {
        new BridgeExporterRequest.Builder().withExportType(ExportType.HOURLY).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "Must not specify endDateTime for instant export.")
    public void instantWithEndDateTIme() {
        new BridgeExporterRequest.Builder().withExportType(ExportType.INSTANT).withEndDateTime(END_DATE_TIME).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "Must specify study whitelist for instant export.")
    public void instantWithNoWhitelist() {
        new BridgeExporterRequest.Builder().withExportType(ExportType.INSTANT).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "Cannot specify recordIdS3Override for daily, hourly or instant export.")
    public void dailyWithOverride() {
        new BridgeExporterRequest.Builder().withExportType(DAILY).withEndDateTime(END_DATE_TIME).withRecordIdS3Override(TEST_RECORD_OVERRIDE).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "Cannot specify recordIdS3Override for daily, hourly or instant export.")
    public void houlyWithOverride() {
        new BridgeExporterRequest.Builder().withExportType(ExportType.HOURLY).withEndDateTime(END_DATE_TIME).withStudyWhitelist(STUDY_WHITELIST).withRecordIdS3Override(TEST_RECORD_OVERRIDE).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "Cannot specify recordIdS3Override for daily, hourly or instant export.")
    public void instantWithOverride() {
        new BridgeExporterRequest.Builder().withExportType(ExportType.INSTANT).withStudyWhitelist(STUDY_WHITELIST).withRecordIdS3Override(TEST_RECORD_OVERRIDE).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "Must specify recordIdS3Override for override export.")
    public void withNoRecordSources() {
        new BridgeExporterRequest.Builder().build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "Must specify recordIdS3Override for override export.")
    public void emptyRecordOverride() {
        new BridgeExporterRequest.Builder().withRecordIdS3Override("").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "Must specify recordIdS3Override for override export.")
    public void blankRecordOverride() {
        new BridgeExporterRequest.Builder().withRecordIdS3Override("   ").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "exporterDdbPrefixOverride and synapseProjectOverrideMap must both be specified or both be absent.")
    public void ddbPrefixOverrideWithoutProjectOverride() {
        new BridgeExporterRequest.Builder().withEndDateTime(END_DATE_TIME).withExportType(DAILY).withExporterDdbPrefixOverride(TEST_DDB_PREFIX_OVERRIDE)
                .build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "exporterDdbPrefixOverride and synapseProjectOverrideMap must both be specified or both be absent.")
    public void projectOverrideWithoutDdbPrefixOverride() {
        new BridgeExporterRequest.Builder().withEndDateTime(END_DATE_TIME).withExportType(DAILY)
                .withSynapseProjectOverrideMap(TEST_PROJECT_OVERRIDE_MAP).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "exporterDdbPrefixOverride and synapseProjectOverrideMap must both be specified or both be absent.")
    public void emptyDdbPrefixOverride() {
        new BridgeExporterRequest.Builder().withEndDateTime(END_DATE_TIME).withExportType(DAILY).withExporterDdbPrefixOverride("")
                .withSynapseProjectOverrideMap(TEST_PROJECT_OVERRIDE_MAP).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "exporterDdbPrefixOverride and synapseProjectOverrideMap must both be specified or both be absent.")
    public void blankDdbPrefixOverride() {
        new BridgeExporterRequest.Builder().withEndDateTime(END_DATE_TIME).withExportType(DAILY).withExporterDdbPrefixOverride("   ")
                .withSynapseProjectOverrideMap(TEST_PROJECT_OVERRIDE_MAP).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "If synapseProjectOverrideMap is specified, it can't be empty.")
    public void emptyProjectOverrideMap() {
        new BridgeExporterRequest.Builder().withEndDateTime(END_DATE_TIME).withExportType(DAILY).withExporterDdbPrefixOverride(TEST_DDB_PREFIX_OVERRIDE)
                .withSynapseProjectOverrideMap(ImmutableMap.of()).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "If studyWhitelist is specified, it can't be empty.")
    public void emptyStudyWhitelist() {
        new BridgeExporterRequest.Builder().withEndDateTime(END_DATE_TIME).withExportType(DAILY).withStudyWhitelist(ImmutableSet.of()).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "If tableWhitelist is specified, it can't be empty.")
    public void emptyTableWhitelist() {
        new BridgeExporterRequest.Builder().withEndDateTime(END_DATE_TIME).withExportType(DAILY).withTableWhitelist(ImmutableSet.of()).build();
    }

    @Test
    public void jsonSerializationWithEndDateTimes() throws Exception {
        // start with JSON
        String jsonText = "{\n" +
                "   \"endDateTime\":\"" + END_DATE_TIME_STRING + "\",\n" +
                "   \"exportType\":\"" + HOURLY + "\",\n" +
                "   \"studyWhitelist\":[\"test-study\"]\n" +
                "}";

        // convert to POJO
        BridgeExporterRequest request = DefaultObjectMapper.INSTANCE.readValue(jsonText, BridgeExporterRequest.class);
        assertEquals(request.getEndDateTime(), END_DATE_TIME);
        assertEquals(request.getSharingMode(), BridgeExporterSharingMode.SHARED);

        // Convert back to JSON. Also make sure the JSON doesn't have null fields. (Just check record override. No need
        // to check them all.
        JsonNode jsonNode = DefaultObjectMapper.INSTANCE.convertValue(request, JsonNode.class);
        assertEquals(DateTime.parse(jsonNode.get("endDateTime").textValue()), END_DATE_TIME);
        assertEquals(jsonNode.get("exportType").textValue(), HOURLY.toString());
        assertFalse(jsonNode.has("recordS3Override"));
        assertEquals(jsonNode.get("sharingMode").textValue(), BridgeExporterSharingMode.SHARED.name());

        JsonNode studyWhitelistNode = jsonNode.get("studyWhitelist");
        assertTrue(studyWhitelistNode.isArray());
        assertEquals(studyWhitelistNode.size(), 1);
        assertEquals(studyWhitelistNode.get(0).textValue(), "test-study");
    }

    @Test
    public void jsonSerializationWithOptionalParams() throws Exception {
        // start with JSON
        String jsonText = "{\n" +
                "   \"exporterDdbPrefixOverride\":\"" + TEST_DDB_PREFIX_OVERRIDE + "\",\n" +
                "   \"recordIdS3Override\":\"" + TEST_RECORD_OVERRIDE + "\",\n" +
                "   \"redriveCount\":2,\n" +
                "   \"sharingMode\":\"PUBLIC_ONLY\",\n" +
                "   \"studyWhitelist\":[\"test-study\"],\n" +
                "   \"synapseProjectOverrideMap\":{\n" +
                "       \"test-study\":\"test-project-id\"\n" +
                "   },\n" +
                "   \"tableWhitelist\":[{\n" +
                "       \"studyId\":\"test-study\",\n" +
                "       \"schemaId\":\"test-schema\",\n" +
                "       \"revision\":13\n" +
                "   }],\n" +
                "   \"tag\":\"" + TEST_TAG + "\"\n" +
                "}";

        // convert to POJO
        BridgeExporterRequest request = DefaultObjectMapper.INSTANCE.readValue(jsonText, BridgeExporterRequest.class);
        assertEquals(request.getExporterDdbPrefixOverride(), TEST_DDB_PREFIX_OVERRIDE);
        assertEquals(request.getRecordIdS3Override(), TEST_RECORD_OVERRIDE);
        assertEquals(request.getRedriveCount(), 2);
        assertEquals(request.getSharingMode(), BridgeExporterSharingMode.PUBLIC_ONLY);
        assertEquals(request.getStudyWhitelist(), ImmutableSet.of("test-study"));
        assertEquals(request.getSynapseProjectOverrideMap(), TEST_PROJECT_OVERRIDE_MAP);
        assertEquals(request.getTableWhitelist(), ImmutableSet.of(TEST_SCHEMA_KEY));
        assertEquals(request.getTag(), TEST_TAG);

        // convert back to JSON
        JsonNode jsonNode = DefaultObjectMapper.INSTANCE.convertValue(request, JsonNode.class);
        assertFalse(jsonNode.has("date"));
        assertEquals(jsonNode.get("exporterDdbPrefixOverride").textValue(), TEST_DDB_PREFIX_OVERRIDE);
        assertEquals(jsonNode.get("recordIdS3Override").textValue(), TEST_RECORD_OVERRIDE);
        assertEquals(jsonNode.get("redriveCount").intValue(), 2);
        assertEquals(jsonNode.get("sharingMode").textValue(), BridgeExporterSharingMode.PUBLIC_ONLY.name());
        assertEquals(jsonNode.get("tag").textValue(), TEST_TAG);

        JsonNode studyWhitelistNode = jsonNode.get("studyWhitelist");
        assertTrue(studyWhitelistNode.isArray());
        assertEquals(studyWhitelistNode.size(), 1);
        assertEquals(studyWhitelistNode.get(0).textValue(), "test-study");

        JsonNode projectOverrideMapNode = jsonNode.get("synapseProjectOverrideMap");
        assertTrue(projectOverrideMapNode.isObject());
        assertEquals(projectOverrideMapNode.size(), 1);
        assertEquals(projectOverrideMapNode.get("test-study").textValue(), "test-project-id");

        JsonNode tableWhitelistNode = jsonNode.get("tableWhitelist");
        assertTrue(tableWhitelistNode.isArray());
        assertEquals(tableWhitelistNode.size(), 1);
        assertTrue(tableWhitelistNode.get(0).isObject());
        assertEquals(tableWhitelistNode.get(0).size(), 3);
        assertEquals(tableWhitelistNode.get(0).get("studyId").textValue(), "test-study");
        assertEquals(tableWhitelistNode.get(0).get("schemaId").textValue(), "test-schema");
        assertEquals(tableWhitelistNode.get(0).get("revision").intValue(), 13);
    }

    @Test
    public void equalsVerifier() {
        EqualsVerifier.forClass(BridgeExporterRequest.class).allFieldsShouldBeUsed().verify();
    }
}
