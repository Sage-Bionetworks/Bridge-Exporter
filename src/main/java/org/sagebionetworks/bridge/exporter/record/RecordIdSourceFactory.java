package org.sagebionetworks.bridge.exporter.record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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
import org.joda.time.Interval;
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
    static final String LAST_EXPORT_DATE_TIME = "lastExportDateTime";
    static final String UPLOADED_ON = "uploadedOn";

    // config vars
    private String overrideBucket;
    private DateTimeZone timeZone;

    // Spring helpers
    private DynamoQueryHelper ddbQueryHelper;
    private Index ddbRecordStudyUploadedOnIndex;
    private Index ddbRecordUploadDateIndex;
    private S3Helper s3Helper;
    private Table ddbExportTimeTable;
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

    /** DDB Record table Upload Date index. */
    @Resource(name = "ddbRecordUploadDateIndex")
    final void setDdbRecordUploadDateIndex(Index ddbRecordUploadDateIndex) {
        this.ddbRecordUploadDateIndex = ddbRecordUploadDateIndex;
    }

    /** DDB Export Time Table. */
    @Resource(name = "ddbExportTimeTable")
    final void setDdbExportTimeTable(Table ddbExportTimeTable) {
        this.ddbExportTimeTable = ddbExportTimeTable;
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
        if (request.getStudyWhitelist() != null) {
            recordItemIter = getDynamoRecordIdSourceWithStudyWhiteList(request, studyIdsToQuery);
        } else {
            recordItemIter = getDynamoRecordIdSourceWithoutStudyWhiteList(request, studyIdsToQuery);
        }

        System.out.println("returned record items: " + recordItemIter);


        // finally, update studies in export time table
        // right now, the studyIdsToQuery contains all modified study ids
        // only if it is not a re-export need it to update the ddb table
        if (!request.getReExport()) {
            for (String studyId: studyIdsToQuery.keySet()) {
                System.out.println(studyId);
                ddbExportTimeTable.putItem(new Item().withPrimaryKey(STUDY_ID, studyId).withNumber(LAST_EXPORT_DATE_TIME, endDateTime));
            }
        }

        return new RecordIdSource<>(recordItemIter, DYNAMO_ITEM_CONVERTER);
    }

    /**
     * Used for one-time exporting and any other exporting including study white list.
     * @param request
     * @param studyIdsToQuery
     * @return
     */
    private Iterable<Item> getDynamoRecordIdSourceWithStudyWhiteList(BridgeExporterRequest request, Map<String, String> studyIdsToQuery) {
        DateTime endDateTime = request.getEndDateTime();

        // proceed
        Iterable<Item> recordItemIter;
        long endEpochTime = endDateTime.getMillis() - 1;

        // We need to make a separate query for _each_ study in the whitelist. That's just how DDB hash keys work.
        List<Iterable<Item>> recordItemIterList = new ArrayList<>();
        for (String oneStudyId : studyIdsToQuery.keySet()) {
            Iterable<Item> recordItemIterTemp = ddbQueryHelper.query(ddbRecordStudyUploadedOnIndex, "studyId", oneStudyId,
                    new RangeKeyCondition("uploadedOn").between(Long.valueOf(studyIdsToQuery.get(oneStudyId)),
                            endEpochTime));
            recordItemIterList.add(recordItemIterTemp);
        }
        recordItemIter = Iterables.concat(recordItemIterList);

        return recordItemIter;
    }

    /**
     * Only used for daily exporting
     * @param request
     * @return
     */
    private Iterable<Item> getDynamoRecordIdSourceWithoutStudyWhiteList(BridgeExporterRequest request, Map<String, String> studyIdsToQuery) {
        DateTime endDateTime = request.getEndDateTime();
        DateTime startDateTime = request.getStartDateTime();

        // proceed
        Iterable<Item> recordItemIter;

        // always query a list of records within given time range
        recordItemIter = ddbQueryHelper.query(ddbRecordUploadDateIndex, "uploadDate",
                request.getDate().toString());

        List<Item> retRecordItems = new ArrayList<>();

        // for each study, check last export date time
        for (Item recordItem : recordItemIter) {
            String studyId = recordItem.getString(STUDY_ID);
            if (!studyIdsToQuery.containsKey(studyId)) {
                studyIdsToQuery.put(studyId, String.valueOf(startDateTime.getMillis()));
            }

            // filter as well
            DateTime uploadedOn = new DateTime(recordItem.getLong(UPLOADED_ON), timeZone);
            // exclusive end date time
            Interval dateTimeRange = new Interval(new DateTime(Long.valueOf(studyIdsToQuery.get(studyId)), timeZone), endDateTime);
            if (dateTimeRange.contains(uploadedOn)) {
                retRecordItems.add(recordItem);
            }
        }

        return retRecordItems;
    }

    /**
     * Helper method to generate study ids for query
     * @param request
     * @return
     */
    private Map<String, String> bootstrapStudyIdsToQuery(BridgeExporterRequest request) {
        DateTime startDateTime = request.getStartDateTime();

        Map<String, String> studyIdWithLastExportDateTime = new HashMap<>();

        // only if the request is not a re-export need it to look up ddb table
        if (request.getStudyWhitelist() == null && !request.getReExport()) {
            // get the export time table from ddb as a whole
            ScanRequest scanRequest = new ScanRequest()
                    .withTableName(ddbExportTimeTable.getTableName());

            ScanResult result = ddbClientScan.scan(scanRequest);

            // convert the list of map to a map with key is studyid and value is lastExportDateTime
            for (Map<String, AttributeValue> item : result.getItems()) {
                studyIdWithLastExportDateTime.put(item.get(STUDY_ID).getS(), item.get(LAST_EXPORT_DATE_TIME).getN());
            }
        }

        System.out.println(studyIdWithLastExportDateTime);

        Map<String, String> studyIdsToQuery = new TreeMap<>();

        // then, check if it has study white list
        if (request.getStudyWhitelist() != null) {
            for (String studyId : request.getStudyWhitelist()) {
                Item studyIdItem = ddbExportTimeTable.getItem(STUDY_ID, studyId);
                if (studyIdItem != null && !request.getReExport()) {
                    studyIdsToQuery.put(studyId, String.valueOf(studyIdItem.getLong(LAST_EXPORT_DATE_TIME)));
                } else {
                    // bootstrap with the startDateTime in request
                    studyIdsToQuery.put(studyId, String.valueOf(startDateTime.getMillis()));
                }
            }
        } else {
            studyIdsToQuery.putAll(studyIdWithLastExportDateTime);
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
