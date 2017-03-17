package org.sagebionetworks.bridge.exporter.record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Resource;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.google.common.collect.Iterables;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.dynamodb.DynamoQueryHelper;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.s3.S3Helper;

/**
 * Factory class to construct the appropriate RecordIdSource for the given request. This class abstracts away logic for
 * initializing a RecordIdSource from a DynamoDB query or from a record override file in S3.
 */
@Component
public class RecordIdSourceFactory {
    private static final RecordIdSource.Converter<Item> DYNAMO_ITEM_CONVERTER = from -> from.getString("id");
    private static final RecordIdSource.Converter<String> NOOP_CONVERTER = from -> from;
    static final String STUDY_ID = "studyId";
    static final String IDENTIFIER = "identifier";
    static final String LAST_EXPORT_DATE_TIME = "lastExportDateTime";

    // config vars
    private String overrideBucket;
    private DateTimeZone timeZone;

    // Spring helpers
    private DynamoQueryHelper ddbQueryHelper;
    private Index ddbRecordStudyUploadedOnIndex;
    private S3Helper s3Helper;
    private Table ddbExportTimeTable;
    private Table ddbStudyTable;
    private AmazonDynamoDBClient ddbClientScan;

    /** Config, used to get S3 bucket for record ID override files. */
    @Autowired
    final void setConfig(Config config) {
        overrideBucket = config.get(BridgeExporterUtil.CONFIG_KEY_RECORD_ID_OVERRIDE_BUCKET);
        timeZone = DateTimeZone.forID(config.get(BridgeExporterUtil.CONFIG_KEY_TIME_ZONE_NAME));
    }

    /** DDB Query Helper, used to abstract away query logic. */
    @Autowired
    final void setDdbQueryHelper(DynamoQueryHelper ddbQueryHelper) {
        this.ddbQueryHelper = ddbQueryHelper;
    }

    /** DDB Record table studyId-uploadedOn index. */
    @Resource(name = "ddbRecordStudyUploadedOnIndex")
    final void setDdbRecordStudyUploadedOnIndex(Index ddbRecordStudyUploadedOnIndex) {
        this.ddbRecordStudyUploadedOnIndex = ddbRecordStudyUploadedOnIndex;
    }

    /** DDB Export Time Table. */
    @Resource(name = "ddbExportTimeTable")
    final void setDdbExportTimeTable(Table ddbExportTimeTable) {
        this.ddbExportTimeTable = ddbExportTimeTable;
    }

    @Resource(name = "ddbStudyTable")
    public final void setDdbStudyTable(Table ddbStudyTable) {
        this.ddbStudyTable = ddbStudyTable;
    }

    @Resource(name = "ddbClientScan")
    final void setDdbClientScan(AmazonDynamoDBClient ddbClientScan) {
        this.ddbClientScan = ddbClientScan;
    }


    /** S3 Helper, used to download record ID override files. */
    @Autowired
    final void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    /**
     * Gets the record ID source for the given Bridge EX request. Returns an Iterable instead of a RecordIdSource for
     * easy mocking.
     *
     * @param request
     *         Bridge EX request
     * @return record ID source
     * @throws IOException
     *         if we fail reading the underlying source
     */
    public Iterable<String> getRecordSourceForRequest(BridgeExporterRequest request) throws IOException {
        if (StringUtils.isNotBlank(request.getRecordIdS3Override())) {
            return getS3RecordIdSource(request);
        } else {
            return getDynamoRecordIdSourceGeneral(request);
        }
    }

    /**
     * Helper method to get ddb records
     * @param request
     * @return
     */
    private Iterable<String> getDynamoRecordIdSourceGeneral(BridgeExporterRequest request) {
        Map<String, String> studyIdsToQuery = bootstrapStudyIdsToQuery(request);

        long endDateTime = request.getEndDateTime().getMillis();

        // proceed
        Iterable<Item> recordItemIter;
        long endEpochTime = endDateTime - 1;

        // We need to make a separate query for _each_ study in the whitelist. That's just how DDB hash keys work.
        List<Iterable<Item>> recordItemIterList = new ArrayList<>();
        for (String oneStudyId : studyIdsToQuery.keySet()) {
            Iterable<Item> recordItemIterTemp = ddbQueryHelper.query(ddbRecordStudyUploadedOnIndex, "studyId", oneStudyId,
                    new RangeKeyCondition("uploadedOn").between(Long.valueOf(studyIdsToQuery.get(oneStudyId)),
                            endEpochTime));
            recordItemIterList.add(recordItemIterTemp);
        }
        recordItemIter = Iterables.concat(recordItemIterList);

        // finally, update studies in export time table
        // right now, the studyIdsToQuery contains all modified study ids
        // even if a study has no record to export during given time range,
        // we still update its last export date time to the newest one for convenience.
        // only if it is not a re-export needs it to update the ddb table
        if (!request.getReExport()) {
            for (String studyId: studyIdsToQuery.keySet()) {
                ddbExportTimeTable.putItem(new Item().withPrimaryKey(STUDY_ID, studyId).withNumber(LAST_EXPORT_DATE_TIME, endDateTime));
            }
        }

        return new RecordIdSource<>(recordItemIter, DYNAMO_ITEM_CONVERTER);
    }

    /**
     * Helper method to generate study ids for query
     * @param request
     * @return
     */
    private Map<String, String> bootstrapStudyIdsToQuery(BridgeExporterRequest request) {
        DateTime startDateTime = request.getStartDateTime();

        List<String> studyIdList = new ArrayList<>();

        if (request.getStudyWhitelist() == null) {
            // get the study id list from ddb table
            ScanRequest scanRequest = new ScanRequest()
                    .withTableName(ddbStudyTable.getTableName());

            ScanResult result = ddbClientScan.scan(scanRequest);

            for (Map<String, AttributeValue> item : result.getItems()) {
                studyIdList.add(item.get(IDENTIFIER).getS());
            }
        } else {
            studyIdList.addAll(request.getStudyWhitelist());
        }

        // maintain insert order for testing
        Map<String, String> studyIdsToQuery = new LinkedHashMap<>();

        for (String studyId : studyIdList) {
            Item studyIdItem = ddbExportTimeTable.getItem(STUDY_ID, studyId);
            if (studyIdItem != null && !request.getReExport()) {
                studyIdsToQuery.put(studyId, String.valueOf(studyIdItem.getLong(LAST_EXPORT_DATE_TIME)));
            } else {
                // bootstrap with the startDateTime in request
                studyIdsToQuery.put(studyId, String.valueOf(startDateTime.getMillis()));
            }
        }

        return studyIdsToQuery;
    }

    /**
     * Get the record ID source from a record override file in S3. We assume the list of record IDs is small enough to
     * reasonably fit in memory.
     */
    private Iterable<String> getS3RecordIdSource(BridgeExporterRequest request) throws IOException {
        List<String> recordIdList = s3Helper.readS3FileAsLines(overrideBucket, request.getRecordIdS3Override());
        return new RecordIdSource<>(recordIdList, NOOP_CONVERTER);
    }
}
