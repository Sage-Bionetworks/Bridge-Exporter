package org.sagebionetworks.bridge.exporter;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3Client;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.joda.time.LocalDate;
import org.joda.time.format.ISODateTimeFormat;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;

import org.sagebionetworks.bridge.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.worker.ExportTask;
import org.sagebionetworks.bridge.worker.ExportWorkerManager;

public class BridgeExporter {
    private static final Joiner SCHEMAS_NOT_FOUND_JOINER = Joiner.on(", ");
    private static final String SHARING_SCOPE_NO_SHARING = "NO_SHARING";
    private static final String SHARING_SCOPE_SPARSE = "SPONSORS_AND_PARTNERS";
    private static final String SHARING_SCOPE_BROAD = "ALL_QUALIFIED_RESEARCHERS";
    private static final Set<String> VALID_SHARING_SCOPE_SET = ImmutableSet.of(SHARING_SCOPE_NO_SHARING,
            SHARING_SCOPE_SPARSE, SHARING_SCOPE_BROAD);
    private static final TypeReference<List<UploadSchemaKey>> TYPE_REF_SCHEMA_KEY_LIST =
            new TypeReference<List<UploadSchemaKey>>() {
            };

    // Number of records before the script stops processing records. This is used for testing. To make this unlimited,
    // set it to -1.
    private static final int RECORD_LIMIT = -1;

    // Script should report progress after this many records, so users tailing the logs can see that it's still
    // making progress
    private static final int PROGRESS_REPORT_PERIOD = 1000;

    public static void main(String[] args) throws IOException {
        try {
            System.out.println("[METRICS] Starting Bridge Exporter for args " + Joiner.on(' ').join(args));

            BridgeExporter bridgeExporter = new BridgeExporter();
            if ("redrive-tables".equals(args[0])) {
                // format: BridgeExporter redrive-tables [table list file] yyyy-mm-dd
                bridgeExporter.setDate(args[2]);
                bridgeExporter.setMode(ExportMode.REDRIVE_TABLES);
                bridgeExporter.setRedriveTableKeySet(extractRedriveTableKeySet(args[1]));
            } else if ("redrive-records".equals(args[0])) {
                // format: BridgeExporter redrive-records [record list file]
                // Note that redrive-records doesn't need an upload date, since it doesn't talk to the uploadDate index
                bridgeExporter.setMode(ExportMode.REDRIVE_RECORDS);
                bridgeExporter.setRecordIdFilename(args[1]);

                // TODO: refactor this somehow so we don't end up copy-pasting this everywhere
                // extra flags
                if (args.length >= 3) {
                    if ("--public-only".equals(args[2])) {
                        System.out.println("[METRICS] Public-Only Mode");
                        bridgeExporter.setShouldExportSparse(false);
                    } else {
                        throw new IllegalArgumentException("Unknown arg: " + args[1]);
                    }
                }
            } else {
                // format: BridgeExporter yyyy-mm-dd
                bridgeExporter.setDate(args[0]);
                bridgeExporter.setMode(ExportMode.EXPORT);

                // extra flags
                if (args.length >= 2) {
                    if ("--public-only".equals(args[1])) {
                        System.out.println("[METRICS] Public-Only Mode");
                        bridgeExporter.setShouldExportSparse(false);
                    } else {
                        throw new IllegalArgumentException("Unknown arg: " + args[1]);
                    }
                }
            }
            bridgeExporter.run();
        } catch (Throwable t) {
            t.printStackTrace(System.out);
        } finally {
            System.exit(0);
        }
    }

    private static Set<UploadSchemaKey> extractRedriveTableKeySet(String filename) throws IOException {
        List<UploadSchemaKey> keyList = BridgeExporterUtil.JSON_MAPPER.readValue(new File(filename),
                TYPE_REF_SCHEMA_KEY_LIST);
        return ImmutableSet.copyOf(keyList);
    }

    private final Map<String, Integer> counterMap = new TreeMap<>();
    private final Map<String, Set<String>> setCounterMap = new TreeMap<>();

    // Configured externally
    private String uploadDateString;
    private ExportMode mode;
    private String recordIdFilename;
    private Set<UploadSchemaKey> redriveTableKeySet;
    private boolean shouldExportSparse = true;

    // Internal state
    private BridgeExporterConfig config;
    private ExportWorkerManager manager;
    private Table participantOptionsTable;
    private RecordIdSource recordIdSource;
    private UploadSchemaHelper schemaHelper;
    private Table recordTable;
    private final Map<String, String> userSharingScopeCache = new HashMap<>();

    public final void setDate(String dateString) {
        // validate date
        LocalDate.parse(dateString);

        this.uploadDateString = dateString;
    }

    public final void setMode(ExportMode mode) {
        this.mode = mode;
    }

    public final void setRecordIdFilename(String recordIdFilename) {
        this.recordIdFilename = recordIdFilename;
    }

    public final void setRedriveTableKeySet(Set<UploadSchemaKey> redriveTableKeySet) {
        this.redriveTableKeySet = ImmutableSet.copyOf(redriveTableKeySet);
    }

    /**
     * Whether "sharing sparsely" data should be exported. Defaults to true. If the --public-only flag is set, this
     * flips to false.
     */
    public final void setShouldExportSparse(boolean shouldExportSparse) {
        this.shouldExportSparse = shouldExportSparse;
    }

    public void run() throws BridgeExporterException, IOException, SynapseException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            System.out.println("[METRICS] Starting initialization: " + BridgeExporterUtil.getCurrentLocalTimestamp());
            init();
            System.out.println("[METRICS] Initialization done: " + BridgeExporterUtil.getCurrentLocalTimestamp());

            downloadHealthDataRecords();
        } finally {
            stopwatch.stop();
            System.out.println("[METRICS] Bridge Exporter done: " + BridgeExporterUtil.getCurrentLocalTimestamp());
            System.out.println("[METRICS] Total time: " + stopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
        }
    }

    private void init() throws BridgeExporterException, IOException, SynapseException {
        LocalDate todaysDate = LocalDate.now(BridgeExporterUtil.LOCAL_TIME_ZONE);
        String todaysDateString = todaysDate.toString(ISODateTimeFormat.date());

        File synapseConfigFile = new File(System.getProperty("user.home") + "/bridge-synapse-exporter-config.json");
        config = BridgeExporterUtil.JSON_MAPPER.readValue(synapseConfigFile, BridgeExporterConfig.class);

        // Dynamo DB client - move to Spring
        // This gets credentials from the default credential chain. For developer desktops, this is ~/.aws/credentials.
        // For EC2 instances, this happens transparently.
        // See http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html and
        // http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html#set-up-creds for more
        // info.
        DynamoDB ddbClient = new DynamoDB(new AmazonDynamoDBClient());
        participantOptionsTable = ddbClient.getTable(config.getBridgeDataDdbPrefix() + "ParticipantOptions");
        recordTable = ddbClient.getTable(config.getBridgeDataDdbPrefix() + "HealthDataRecord3");

        // S3 client - move to Spring
        AmazonS3Client s3Client = new AmazonS3Client();
        S3Helper s3Helper = new S3Helper();
        s3Helper.setS3Client(s3Client);

        // Export Helper
        ExportHelper exportHelper = new ExportHelper();
        exportHelper.setConfig(config);
        exportHelper.setDdbClient(ddbClient);
        exportHelper.setS3Helper(s3Helper);

        // synapse client
        SynapseClient synapseClient = new SynapseClientImpl();
        synapseClient.setUserName(config.getUsername());
        synapseClient.setApiKey(config.getApiKey());

        // synapse helper
        SynapseHelper synapseHelper = new SynapseHelper();
        synapseHelper.setBridgeExporterConfig(config);
        synapseHelper.setS3Helper(s3Helper);
        synapseHelper.setSynapseClient(synapseClient);

        // schema Helper
        schemaHelper = new UploadSchemaHelper();
        schemaHelper.setConfig(config);
        schemaHelper.setDdbClient(ddbClient);
        schemaHelper.init();

        // export worker manager
        manager = new ExportWorkerManager();
        manager.setBridgeExporterConfig(config);
        manager.setDdbClient(ddbClient);
        manager.setExportHelper(exportHelper);
        manager.setS3Helper(s3Helper);
        manager.setSchemaHelper(schemaHelper);
        if (mode == ExportMode.REDRIVE_TABLES) {
            manager.setSchemaWhitelist(redriveTableKeySet);
        }
        manager.setSynapseHelper(synapseHelper);
        manager.setTodaysDateString(todaysDateString);
        manager.init();

        // record ID source
        switch (mode) {
            case EXPORT:
            case REDRIVE_TABLES:
                // export and redrive tables both get their records from DDB
                DynamoRecordIdSource dynamoRecordIdSource = new DynamoRecordIdSource();
                dynamoRecordIdSource.setConfig(config);
                dynamoRecordIdSource.setDdbClient(ddbClient);
                dynamoRecordIdSource.setUploadDateString(uploadDateString);
                recordIdSource = dynamoRecordIdSource;
                break;
            case REDRIVE_RECORDS:
                FileRecordIdSource fileRecordIdSource = new FileRecordIdSource();
                fileRecordIdSource.setFilename(recordIdFilename);
                recordIdSource = fileRecordIdSource;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported export mode " + mode.name());
        }
        recordIdSource.init();
    }

    private void downloadHealthDataRecords() {
        Stopwatch progressStopwatch = Stopwatch.createStarted();

        // get record IDs from record ID source and query DDB
        int ddbDelay = config.getDdbDelay();
        while (recordIdSource.hasNext()) {
            String recordId = recordIdSource.next();

            // running count of records
            int numTotal = incrementCounter("numTotal");
            if (numTotal % PROGRESS_REPORT_PERIOD == 0) {
                System.out.println("[METRICS] Num records so far: " + numTotal + " in "
                        + progressStopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
            }
            if (RECORD_LIMIT > 0 && numTotal > RECORD_LIMIT) {
                break;
            }

            if (ddbDelay > 0) {
                // sleep to rate limit our requests to DDB
                try {
                    Thread.sleep(ddbDelay);
                } catch (InterruptedException ex) {
                    System.out.println("[ERROR] Interrupted while sleeping: " + ex.getMessage());
                    ex.printStackTrace(System.out);
                }
            }

            try {
                // re-query health data records to get values
                Item oneRecord;
                try {
                    oneRecord = recordTable.getItem("id", recordId);
                } catch (AmazonClientException ex) {
                    System.out.println("[ERROR] Exception querying record for ID " + recordId + ": " + ex.getMessage());
                    continue;
                }
                if (oneRecord == null) {
                    System.out.println("[ERROR] No record for ID " + recordId);
                    continue;
                }

                // basic record data
                String studyId = oneRecord.getString("studyId");
                String schemaId = oneRecord.getString("schemaId");
                int schemaRev = oneRecord.getInt("schemaRevision");
                UploadSchemaKey schemaKey = new UploadSchemaKey(studyId, schemaId, schemaRev);

                // filter out studies that we're not configured for
                if (!config.getStudyIdSet().contains(studyId)) {
                    continue;
                }

                // Filter by version, if needed. (Null defaults to false.)
                // Note: This is specific to the original ResearchKit launch.
                Boolean filterV1 = config.getFilterV1ByStudy().get(studyId);
                if (filterV1 != null && filterV1) {
                    // To be safe, if the appVersion is not specified, filter it out as well.
                    PhoneAppVersionInfo phoneAppVersionInfo = PhoneAppVersionInfo.fromRecord(oneRecord);
                    String appVersion = phoneAppVersionInfo.getAppVersion();
                    if (Strings.isNullOrEmpty(appVersion) || appVersion.startsWith(BridgeExporterUtil.V1_PREFIX)) {
                        incrementCounter("numV1Filtered");
                        continue;
                    }
                }

                // process/filter by user sharing scope
                if (!shouldExport(oneRecord)) {
                    continue;
                }

                String healthCode = oneRecord.getString("healthCode");
                incrementSetCounter("uniqueHealthCodes[" + studyId + "]", healthCode);

                // Multiplex and add the right tasks to the manager. Since Export Tasks are immutable, it's safe to
                // shared the same export task.
                ExportTask task = new ExportTask(schemaKey, oneRecord, null);
                if ("ios-survey".equals(schemaId)) {
                    try {
                        manager.addIosSurveyExportTask(studyId, task);
                    } catch (BridgeExporterException ex) {
                        System.out.println("[ERROR] Error queueing survey task for record " + recordId + " in study "
                                + studyId + ": " + ex.getMessage());
                    }
                } else {
                    try {
                        manager.addHealthDataExportTask(schemaKey, task);
                    } catch (SchemaNotFoundException ex) {
                        System.out.println("[ERROR] Schema not found for record " + recordId + " schema "
                                + schemaKey.toString() + ": " + ex.getMessage());
                    } catch (BridgeExporterException ex) {
                        System.out.println("[ERROR] Error queueing app version task for record " + recordId + " in study "
                                + studyId + ": " + ex.getMessage());
                    }
                }
            } catch (IOException | RuntimeException ex) {
                System.out.println("[ERROR] Unknown error processing record " + recordId + ": " + ex.getMessage());
                ex.printStackTrace(System.out);
            }
        }

        // signal the worker manager that we've reached the end of the stream
        manager.endOfStream();

        for (Map.Entry<String, Integer> oneCounter : counterMap.entrySet()) {
            System.out.println("[METRICS] " + oneCounter.getKey() + ": " + oneCounter.getValue());
        }
        for (Map.Entry<String, Set<String>> oneSetCounter : setCounterMap.entrySet()) {
            System.out.println("[METRICS] " + oneSetCounter.getKey() + ": " + oneSetCounter.getValue().size());
        }

        Set<String> schemasNotFound = new TreeSet<>();
        schemasNotFound.addAll(manager.getSchemasNotFound());
        schemasNotFound.addAll(schemaHelper.getSchemasNotFound());
        if (!schemasNotFound.isEmpty()) {
            System.out.println("[METRICS] The following schemas were referenced but not found: "
                    + SCHEMAS_NOT_FOUND_JOINER.join(schemasNotFound));
        }
    }

    // Given the record, checks the record's sharing scope and the user's sharing scope, and takes the most restrictive
    // of those. If the record should be shared (sharing sparsely or sharing broadly), this returns true. If the record
    // shouldn't be shared ("no_sharing", or blank sharing scope), this returns false.
    private boolean shouldExport(Item record) throws IOException {
        // record sharing scope
        String recordSharingScope = record.getString("userSharingScope");
        if (Strings.isNullOrEmpty(recordSharingScope)) {
            // default to no_sharing
            recordSharingScope = SHARING_SCOPE_NO_SHARING;
        } else {
            // validate sharing scope
            if (!VALID_SHARING_SCOPE_SET.contains(recordSharingScope)) {
                throw new IllegalStateException("Unknown record sharing scope: " + recordSharingScope);
            }
        }

        // user sharing scope, check cache first
        String healthCode = record.getString("healthCode");
        String userSharingScope = userSharingScopeCache.get(healthCode);
        if (Strings.isNullOrEmpty(userSharingScope)) {
            // fall back to participant options table
            Item participantOption = participantOptionsTable.getItem("healthDataCode", healthCode);
            if (participantOption != null) {
                String participantOptionData = participantOption.getString("data");
                @SuppressWarnings("unchecked")
                Map<String, String> participantOptionDataMap = BridgeExporterUtil.JSON_MAPPER.readValue(
                        participantOptionData, Map.class);
                userSharingScope = participantOptionDataMap.get("SHARING_SCOPE");
            } else {
                // no participant options, default to no_sharing
                userSharingScope = SHARING_SCOPE_NO_SHARING;
            }

            if (Strings.isNullOrEmpty(userSharingScope)) {
                // no scope from participant options table, default to no_sharing
                userSharingScope = SHARING_SCOPE_NO_SHARING;
            } else {
                // validate this too
                if (!VALID_SHARING_SCOPE_SET.contains(userSharingScope)) {
                    throw new IllegalStateException("Unknown user sharing scope: " + userSharingScope);
                }
            }

            // write it back into the cache
            userSharingScopeCache.put(healthCode, userSharingScope);
        }

        // determine minimum (most restrictive) sharing scope)
        if (SHARING_SCOPE_NO_SHARING.equals(recordSharingScope) || SHARING_SCOPE_NO_SHARING.equals(userSharingScope)) {
            // must not be exported
            incrementCounter("numNotShared");
            return false;
        } else if (SHARING_SCOPE_SPARSE.equals(recordSharingScope) || SHARING_SCOPE_SPARSE.equals(userSharingScope)) {
            if (shouldExportSparse) {
                incrementCounter("numSharingSparsely-Fixed");
                return true;
            } else {
                incrementCounter("numSharingSparsely (Not Exported)");
                return false;
            }
        } else if (SHARING_SCOPE_BROAD.equals(recordSharingScope) || SHARING_SCOPE_BROAD.equals(userSharingScope)) {
            incrementCounter("numSharingBroadly-Fixed");
            return true;
        } else {
            throw new IllegalStateException("Impossible code path in BridgeExporter.shouldExport()");
        }
    }

    private int incrementCounter(String name) {
        Integer oldValue = counterMap.get(name);
        int newValue;
        if (oldValue == null) {
            newValue = 1;
        } else {
            newValue = oldValue + 1;
        }

        counterMap.put(name, newValue);
        return newValue;
    }

    // Only increments the counter if the value hasn't already been used. Used for things like counting unique health
    // codes.
    private void incrementSetCounter(String name, String value) {
        Set<String> set = setCounterMap.get(name);
        if (set == null) {
            set = new HashSet<>();
            setCounterMap.put(name, set);
        }
        set.add(value);
    }
}
