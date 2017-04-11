package org.sagebionetworks.bridge.exporter.record;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import org.joda.time.DateTime;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exporter.dynamo.DynamoHelper;
import org.sagebionetworks.bridge.exporter.dynamo.SharingScope;
import org.sagebionetworks.bridge.exporter.dynamo.StudyInfo;
import org.sagebionetworks.bridge.exporter.metrics.Metrics;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterSharingMode;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

public class RecordFilterHelperTest {
    private static final String DUMMY_HEALTH_CODE = "dummy-health-code";
    private static final String TEST_STUDY = "test-study";
    private static final StudyInfo TEST_STUDY_INFO = new StudyInfo.Builder().withDataAccessTeamId(1337L)
            .withSynapseProjectId("test-project").build();

    @Test
    public void recordMissingSharingScope() {
        // set up inputs
        Metrics metrics = new Metrics();
        BridgeExporterRequest request = makeRequestBuilder().build();
        Item record = makeRecord(null, TEST_STUDY);

        // execute and validate
        RecordFilterHelper helper = makeRecordFilterHelper(SharingScope.SPONSORS_AND_PARTNERS, true);
        assertTrue(helper.shouldExcludeRecord(metrics, request, record));

        Multiset<String> counterMap = metrics.getCounterMap();
        assertEquals(counterMap.count("accepted[NO_SHARING]"), 0);
        assertEquals(counterMap.count("excluded[NO_SHARING]"), 1);
    }

    @Test
    public void recordMalformedSharingScope() {
        // set up inputs
        Metrics metrics = new Metrics();
        BridgeExporterRequest request = makeRequestBuilder().build();
        Item record = makeRecord(null, TEST_STUDY).withString("userSharingScope", "not a valid sharing scope");

        // execute and validate
        RecordFilterHelper helper = makeRecordFilterHelper(SharingScope.SPONSORS_AND_PARTNERS, true);
        assertTrue(helper.shouldExcludeRecord(metrics, request, record));

        Multiset<String> counterMap = metrics.getCounterMap();
        assertEquals(counterMap.count("accepted[NO_SHARING]"), 0);
        assertEquals(counterMap.count("excluded[NO_SHARING]"), 1);
    }

    // This data provider includes all combinations of record sharing scope, using sharing scope, and request sharing
    // mode. (The fifth column is the minimal sharing scope, to help with tests.)
    // Note that we don't need to test missing user participant options, since DynamoHelper takes care of that for us
    // and is tested elsewhere
    @DataProvider(name = "filterBySharingScopeProvider")
    public Object[][] filterBySharingScopeProvider() {
        return new Object[][] {
                { SharingScope.NO_SHARING, SharingScope.NO_SHARING, BridgeExporterSharingMode.ALL, false,
                        SharingScope.NO_SHARING },
                { SharingScope.NO_SHARING, SharingScope.NO_SHARING, BridgeExporterSharingMode.SHARED, true,
                        SharingScope.NO_SHARING },
                { SharingScope.NO_SHARING, SharingScope.NO_SHARING, BridgeExporterSharingMode.PUBLIC_ONLY, true,
                        SharingScope.NO_SHARING },
                { SharingScope.NO_SHARING, SharingScope.SPONSORS_AND_PARTNERS, BridgeExporterSharingMode.ALL, false,
                        SharingScope.NO_SHARING },
                { SharingScope.NO_SHARING, SharingScope.SPONSORS_AND_PARTNERS, BridgeExporterSharingMode.SHARED, true,
                        SharingScope.NO_SHARING },
                { SharingScope.NO_SHARING, SharingScope.SPONSORS_AND_PARTNERS, BridgeExporterSharingMode.PUBLIC_ONLY,
                        true, SharingScope.NO_SHARING },
                { SharingScope.NO_SHARING, SharingScope.ALL_QUALIFIED_RESEARCHERS, BridgeExporterSharingMode.ALL,
                        false, SharingScope.NO_SHARING },
                { SharingScope.NO_SHARING, SharingScope.ALL_QUALIFIED_RESEARCHERS, BridgeExporterSharingMode.SHARED,
                        true, SharingScope.NO_SHARING },
                { SharingScope.NO_SHARING, SharingScope.ALL_QUALIFIED_RESEARCHERS,
                        BridgeExporterSharingMode.PUBLIC_ONLY, true, SharingScope.NO_SHARING },
                { SharingScope.SPONSORS_AND_PARTNERS, SharingScope.NO_SHARING, BridgeExporterSharingMode.ALL, false,
                        SharingScope.NO_SHARING },
                { SharingScope.SPONSORS_AND_PARTNERS, SharingScope.NO_SHARING, BridgeExporterSharingMode.SHARED, true,
                        SharingScope.NO_SHARING },
                { SharingScope.SPONSORS_AND_PARTNERS, SharingScope.NO_SHARING, BridgeExporterSharingMode.PUBLIC_ONLY,
                        true, SharingScope.NO_SHARING },
                { SharingScope.SPONSORS_AND_PARTNERS, SharingScope.SPONSORS_AND_PARTNERS,
                        BridgeExporterSharingMode.ALL, false, SharingScope.SPONSORS_AND_PARTNERS },
                { SharingScope.SPONSORS_AND_PARTNERS, SharingScope.SPONSORS_AND_PARTNERS,
                        BridgeExporterSharingMode.SHARED, false, SharingScope.SPONSORS_AND_PARTNERS },
                { SharingScope.SPONSORS_AND_PARTNERS, SharingScope.SPONSORS_AND_PARTNERS,
                        BridgeExporterSharingMode.PUBLIC_ONLY, true, SharingScope.SPONSORS_AND_PARTNERS },
                { SharingScope.SPONSORS_AND_PARTNERS, SharingScope.ALL_QUALIFIED_RESEARCHERS,
                        BridgeExporterSharingMode.ALL, false, SharingScope.SPONSORS_AND_PARTNERS },
                { SharingScope.SPONSORS_AND_PARTNERS, SharingScope.ALL_QUALIFIED_RESEARCHERS,
                        BridgeExporterSharingMode.SHARED, false, SharingScope.SPONSORS_AND_PARTNERS },
                { SharingScope.SPONSORS_AND_PARTNERS, SharingScope.ALL_QUALIFIED_RESEARCHERS,
                        BridgeExporterSharingMode.PUBLIC_ONLY, true, SharingScope.SPONSORS_AND_PARTNERS },
                { SharingScope.ALL_QUALIFIED_RESEARCHERS, SharingScope.NO_SHARING, BridgeExporterSharingMode.ALL, false,
                        SharingScope.NO_SHARING },
                { SharingScope.ALL_QUALIFIED_RESEARCHERS, SharingScope.NO_SHARING, BridgeExporterSharingMode.SHARED, true,
                        SharingScope.NO_SHARING },
                { SharingScope.ALL_QUALIFIED_RESEARCHERS, SharingScope.NO_SHARING, BridgeExporterSharingMode.PUBLIC_ONLY,
                        true, SharingScope.NO_SHARING },
                { SharingScope.ALL_QUALIFIED_RESEARCHERS, SharingScope.SPONSORS_AND_PARTNERS,
                        BridgeExporterSharingMode.ALL, false, SharingScope.SPONSORS_AND_PARTNERS },
                { SharingScope.ALL_QUALIFIED_RESEARCHERS, SharingScope.SPONSORS_AND_PARTNERS,
                        BridgeExporterSharingMode.SHARED, false, SharingScope.SPONSORS_AND_PARTNERS },
                { SharingScope.ALL_QUALIFIED_RESEARCHERS, SharingScope.SPONSORS_AND_PARTNERS,
                        BridgeExporterSharingMode.PUBLIC_ONLY, true, SharingScope.SPONSORS_AND_PARTNERS },
                { SharingScope.ALL_QUALIFIED_RESEARCHERS, SharingScope.ALL_QUALIFIED_RESEARCHERS,
                        BridgeExporterSharingMode.ALL, false, SharingScope.ALL_QUALIFIED_RESEARCHERS },
                { SharingScope.ALL_QUALIFIED_RESEARCHERS, SharingScope.ALL_QUALIFIED_RESEARCHERS,
                        BridgeExporterSharingMode.SHARED, false, SharingScope.ALL_QUALIFIED_RESEARCHERS },
                { SharingScope.ALL_QUALIFIED_RESEARCHERS, SharingScope.ALL_QUALIFIED_RESEARCHERS,
                        BridgeExporterSharingMode.PUBLIC_ONLY, false, SharingScope.ALL_QUALIFIED_RESEARCHERS },
        };
    }

    @Test(dataProvider = "filterBySharingScopeProvider")
    public void filterBySharingScope(SharingScope recordSharingScope, SharingScope userSharingScope,
            BridgeExporterSharingMode requestSharingMode, boolean expected, SharingScope minimalSharingScope) {
        // set up inputs
        Metrics metrics = new Metrics();
        BridgeExporterRequest request = makeRequestBuilder().withSharingMode(requestSharingMode).build();
        Item record = makeRecord(recordSharingScope, TEST_STUDY);

        // execute and validate
        RecordFilterHelper helper = makeRecordFilterHelper(userSharingScope, true);
        assertEquals(helper.shouldExcludeRecord(metrics, request, record), expected);

        Multiset<String> counterMap = metrics.getCounterMap();
        assertEquals(counterMap.count("accepted[" + minimalSharingScope.name() + "]"), expected ? 0 : 1);
        assertEquals(counterMap.count("excluded[" + minimalSharingScope.name() + "]"), expected ? 1 : 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void recordMissingStudyId() {
        // set up inputs
        Metrics metrics = new Metrics();
        BridgeExporterRequest request = makeRequestBuilder().withStudyWhitelist(ImmutableSet.of(TEST_STUDY)).build();
        Item record = makeRecord(SharingScope.ALL_QUALIFIED_RESEARCHERS, null);

        // execute and validate
        RecordFilterHelper helper = makeRecordFilterHelper(SharingScope.ALL_QUALIFIED_RESEARCHERS, true);
        helper.shouldExcludeRecord(metrics, request, record);
    }

    @Test
    public void studyFilterAccepts() {
        // set up inputs
        Metrics metrics = new Metrics();
        BridgeExporterRequest request = makeRequestBuilder().withStudyWhitelist(ImmutableSet.of(TEST_STUDY)).build();
        Item record = makeRecord(SharingScope.ALL_QUALIFIED_RESEARCHERS, TEST_STUDY);

        // execute and validate
        RecordFilterHelper helper = makeRecordFilterHelper(SharingScope.ALL_QUALIFIED_RESEARCHERS, true);
        assertFalse(helper.shouldExcludeRecord(metrics, request, record));

        Multiset<String> counterMap = metrics.getCounterMap();
        assertEquals(counterMap.count("accepted[test-study]"), 1);
        assertEquals(counterMap.count("excluded[test-study]"), 0);
    }

    @Test
    public void studyFilterExcludes() {
        // set up inputs
        Metrics metrics = new Metrics();
        BridgeExporterRequest request = makeRequestBuilder().withStudyWhitelist(ImmutableSet.of(TEST_STUDY)).build();
        Item record = makeRecord(SharingScope.ALL_QUALIFIED_RESEARCHERS, "excluded-study");

        // execute and validate
        RecordFilterHelper helper = makeRecordFilterHelper(SharingScope.ALL_QUALIFIED_RESEARCHERS, true);
        assertTrue(helper.shouldExcludeRecord(metrics, request, record));

        Multiset<String> counterMap = metrics.getCounterMap();
        assertEquals(counterMap.count("accepted[excluded-study]"), 0);
        assertEquals(counterMap.count("excluded[excluded-study]"), 1);
    }

    @Test
    public void tableFilterAccepts() {
        // set up inputs
        Metrics metrics = new Metrics();
        UploadSchemaKey acceptedSchemaKey = new UploadSchemaKey.Builder().withStudyId(TEST_STUDY)
                .withSchemaId("test-schema").withRevision(3).build();
        BridgeExporterRequest request = makeRequestBuilder().withTableWhitelist(ImmutableSet.of(acceptedSchemaKey))
                .build();
        Item record = makeRecord(SharingScope.ALL_QUALIFIED_RESEARCHERS, TEST_STUDY)
                .withString("schemaId", "test-schema").withInt("schemaRevision", 3);

        // execute and validate
        RecordFilterHelper helper = makeRecordFilterHelper(SharingScope.ALL_QUALIFIED_RESEARCHERS, true);
        assertFalse(helper.shouldExcludeRecord(metrics, request, record));

        Multiset<String> counterMap = metrics.getCounterMap();
        assertEquals(counterMap.count("accepted[test-study-test-schema-v3]"), 1);
        assertEquals(counterMap.count("excluded[test-study-test-schema-v3]"), 0);
    }

    @Test
    public void tableFilterExcludes() {
        // set up inputs
        Metrics metrics = new Metrics();
        UploadSchemaKey acceptedSchemaKey = new UploadSchemaKey.Builder().withStudyId(TEST_STUDY)
                .withSchemaId("test-schema").withRevision(3).build();
        BridgeExporterRequest request = makeRequestBuilder().withTableWhitelist(ImmutableSet.of(acceptedSchemaKey))
                .build();

        // same schema, different revision
        Item record = makeRecord(SharingScope.ALL_QUALIFIED_RESEARCHERS, TEST_STUDY)
                .withString("schemaId", "test-schema").withInt("schemaRevision", 5);

        // execute and validate
        RecordFilterHelper helper = makeRecordFilterHelper(SharingScope.ALL_QUALIFIED_RESEARCHERS, true);
        assertTrue(helper.shouldExcludeRecord(metrics, request, record));

        Multiset<String> counterMap = metrics.getCounterMap();
        assertEquals(counterMap.count("accepted[test-study-test-schema-v5]"), 0);
        assertEquals(counterMap.count("excluded[test-study-test-schema-v5]"), 1);
    }

    // Strictly speaking, this code path has already been covered by all the other tests. However, we're testing it
    // here separately to test metrics.
    @Test
    public void configuredStudy() {
        // set up inputs
        Metrics metrics = new Metrics();
        BridgeExporterRequest request = makeRequestBuilder().build();
        Item record = makeRecord(SharingScope.ALL_QUALIFIED_RESEARCHERS, TEST_STUDY);

        // execute and validate
        RecordFilterHelper helper = makeRecordFilterHelper(SharingScope.ALL_QUALIFIED_RESEARCHERS, true);
        assertFalse(helper.shouldExcludeRecord(metrics, request, record));

        Multiset<String> counterMap = metrics.getCounterMap();
        assertEquals(counterMap.count("configured[test-study]"), 1);
        assertEquals(counterMap.count("unconfigured[test-study]"), 0);
    }

    @Test
    public void unconfiguredStudy() {
        // set up inputs
        Metrics metrics = new Metrics();
        BridgeExporterRequest request = makeRequestBuilder().build();
        Item record = makeRecord(SharingScope.ALL_QUALIFIED_RESEARCHERS, TEST_STUDY);

        // execute and validate
        RecordFilterHelper helper = makeRecordFilterHelper(SharingScope.ALL_QUALIFIED_RESEARCHERS, false);
        assertTrue(helper.shouldExcludeRecord(metrics, request, record));

        Multiset<String> counterMap = metrics.getCounterMap();
        assertEquals(counterMap.count("configured[test-study]"), 0);
        assertEquals(counterMap.count("unconfigured[test-study]"), 1);
    }

    @Test
    public void customExportNoWhitelist() {
        // set up inputs
        Metrics metrics = new Metrics();
        BridgeExporterRequest request = makeRequestBuilder().build();
        Item record = makeRecord(SharingScope.ALL_QUALIFIED_RESEARCHERS, TEST_STUDY);

        // mock DynamoHelper
        DynamoHelper mockDynamoHelper = mock(DynamoHelper.class);
        when(mockDynamoHelper.getSharingScopeForUser(DUMMY_HEALTH_CODE)).thenReturn(
                SharingScope.ALL_QUALIFIED_RESEARCHERS);

        StudyInfo studyInfo = new StudyInfo.Builder().withDataAccessTeamId(1234L).withSynapseProjectId("test-project")
                .withUsesCustomExportSchedule(true).build();
        when(mockDynamoHelper.getStudyInfo(TEST_STUDY)).thenReturn(studyInfo);

        // set up record filter helper
        RecordFilterHelper helper = new RecordFilterHelper();
        helper.setDynamoHelper(mockDynamoHelper);

        // execute and validate
        assertTrue(helper.shouldExcludeRecord(metrics, request, record));

        Multiset<String> counterMap = metrics.getCounterMap();
        assertEquals(counterMap.count("custom-export-accepted[test-study]"), 0);
        assertEquals(counterMap.count("custom-export-excluded[test-study]"), 1);
    }

    @Test
    public void customExportWithWhitelist() {
        // set up inputs
        Metrics metrics = new Metrics();
        BridgeExporterRequest request = makeRequestBuilder().withStudyWhitelist(ImmutableSet.of(TEST_STUDY)).build();
        Item record = makeRecord(SharingScope.ALL_QUALIFIED_RESEARCHERS, TEST_STUDY);

        // mock DynamoHelper
        DynamoHelper mockDynamoHelper = mock(DynamoHelper.class);
        when(mockDynamoHelper.getSharingScopeForUser(DUMMY_HEALTH_CODE)).thenReturn(
                SharingScope.ALL_QUALIFIED_RESEARCHERS);

        StudyInfo studyInfo = new StudyInfo.Builder().withDataAccessTeamId(1234L).withSynapseProjectId("test-project")
                .withUsesCustomExportSchedule(true).build();
        when(mockDynamoHelper.getStudyInfo(TEST_STUDY)).thenReturn(studyInfo);

        // set up record filter helper
        RecordFilterHelper helper = new RecordFilterHelper();
        helper.setDynamoHelper(mockDynamoHelper);

        // execute and validate
        assertFalse(helper.shouldExcludeRecord(metrics, request, record));

        Multiset<String> counterMap = metrics.getCounterMap();
        assertEquals(counterMap.count("custom-export-accepted[test-study]"), 1);
        assertEquals(counterMap.count("custom-export-excluded[test-study]"), 0);
    }

    @Test
    public void customExportWhitelistOtherStudy() {
        // set up inputs
        Metrics metrics = new Metrics();
        BridgeExporterRequest request = makeRequestBuilder().withStudyWhitelist(ImmutableSet.of("other-study"))
                .build();
        Item record = makeRecord(SharingScope.ALL_QUALIFIED_RESEARCHERS, TEST_STUDY);

        // mock DynamoHelper
        DynamoHelper mockDynamoHelper = mock(DynamoHelper.class);
        when(mockDynamoHelper.getSharingScopeForUser(DUMMY_HEALTH_CODE)).thenReturn(
                SharingScope.ALL_QUALIFIED_RESEARCHERS);

        StudyInfo studyInfo = new StudyInfo.Builder().withDataAccessTeamId(1234L).withSynapseProjectId("test-project")
                .withUsesCustomExportSchedule(true).build();
        when(mockDynamoHelper.getStudyInfo(TEST_STUDY)).thenReturn(studyInfo);

        // set up record filter helper
        RecordFilterHelper helper = new RecordFilterHelper();
        helper.setDynamoHelper(mockDynamoHelper);

        // execute and validate
        assertTrue(helper.shouldExcludeRecord(metrics, request, record));

        Multiset<String> counterMap = metrics.getCounterMap();
        assertEquals(counterMap.count("custom-export-accepted[test-study]"), 0);
        assertEquals(counterMap.count("custom-export-excluded[test-study]"), 1);
    }

    @Test
    public void disableExport() {
        // set up inputs
        Metrics metrics = new Metrics();
        BridgeExporterRequest request = makeRequestBuilder().build();
        Item record = makeRecord(SharingScope.ALL_QUALIFIED_RESEARCHERS, TEST_STUDY);

        // mock DynamoHelper
        DynamoHelper mockDynamoHelper = mock(DynamoHelper.class);
        when(mockDynamoHelper.getSharingScopeForUser(DUMMY_HEALTH_CODE)).thenReturn(
                SharingScope.ALL_QUALIFIED_RESEARCHERS);

        StudyInfo studyInfo = new StudyInfo.Builder().withDataAccessTeamId(1234L).withSynapseProjectId("test-project")
                .withDisableExport(true).build();

        when(mockDynamoHelper.getStudyInfo(TEST_STUDY)).thenReturn(studyInfo);

        // set up record filter helper
        RecordFilterHelper helper = new RecordFilterHelper();
        helper.setDynamoHelper(mockDynamoHelper);

        // execute and validate
        assertTrue(helper.shouldExcludeRecord(metrics, request, record));

        Multiset<String> counterMap = metrics.getCounterMap();

        assertEquals(counterMap.count("disabled-export study[test-study]"), 1);
        //assertEquals(counterMap.count("custom-export-excluded[test-study]"), 1);
    }

    private static Item makeRecord(SharingScope recordSharingScope, String studyId) {
        Item record = new Item().with("healthCode", DUMMY_HEALTH_CODE);

        if (recordSharingScope != null) {
            record.withString("userSharingScope", recordSharingScope.name());
        }

        if (studyId != null) {
            record.withString("studyId", studyId);
        }

        return record;
    }

    private static RecordFilterHelper makeRecordFilterHelper(SharingScope userSharingScope,
            boolean isStudyConfigured) {
        // mock DynamoHelper
        DynamoHelper mockDynamoHelper = mock(DynamoHelper.class);
        when(mockDynamoHelper.getSharingScopeForUser(DUMMY_HEALTH_CODE)).thenReturn(userSharingScope);

        if (isStudyConfigured) {
            when(mockDynamoHelper.getStudyInfo(TEST_STUDY)).thenReturn(TEST_STUDY_INFO);
        }

        // set up record filter helper
        RecordFilterHelper helper = new RecordFilterHelper();
        helper.setDynamoHelper(mockDynamoHelper);
        return helper;
    }

    private static BridgeExporterRequest.Builder makeRequestBuilder() {
        DateTime DUMMY_REQUEST_DATE_TIME = DateTime.parse("2015-10-31T23:59:59Z");
        return new BridgeExporterRequest.Builder().withEndDateTime(DUMMY_REQUEST_DATE_TIME).withExportType(ExportType.DAILY)
                .withTag("RecordFilterHelperTest");
    }
}
