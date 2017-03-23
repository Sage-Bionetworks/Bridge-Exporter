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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
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
    private Index ddbRecordUploadDateIndex;
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

    /** DDB Record table Upload Date index. */
    @Resource(name = "ddbRecordUploadDateIndex")
    final void setDdbRecordUploadDateIndex(Index ddbRecordUploadDateIndex) {
        this.ddbRecordUploadDateIndex = ddbRecordUploadDateIndex;
    }

    /** S3 Helper, used to download record ID override files. */
    @Autowired
    final void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
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
    public RecordAndStudyListHolder getRecordSourceForRequest(BridgeExporterRequest request) throws IOException {
        if (StringUtils.isNotBlank(request.getRecordIdS3Override())) {
            return getS3RecordIdSource(request);
        } else if (request.getStudyWhitelist() != null) {
            return getDynamoRecordIdSourceWithStudyWhitelist(request);
        } else {
            return getDynamoRecordIdSource(request);
        }
    }

    /**
     * If we have a study whitelist, we should always use the study-uploadedOn index, as it's more performant. If we
     * have a date, we'll need to compute the start and endDateTimes based off of that. Otherwise, we use the start-
     * and endDateTimes verbatim.
     */
    private RecordAndStudyListHolder getDynamoRecordIdSourceWithStudyWhitelist(BridgeExporterRequest request) {
        // Compute start- and endDateTime.
        DateTime startDateTime;
        DateTime endDateTime;
        if (request.getDate() != null) {
            // startDateTime is obviously date at midnight local time. endDateTime is the start of the next day.
            LocalDate date = request.getDate();
            startDateTime = date.toDateTimeAtStartOfDay(timeZone);
            endDateTime = date.plusDays(1).toDateTimeAtStartOfDay(timeZone);
        } else {
            // Logically, if there is no date, there must be a start- and endDateTime. (We check for recordIdS3Override
            // in the calling method.
            startDateTime = request.getStartDateTime();
            endDateTime = request.getEndDateTime();
        }

        // startDateTime is inclusive but endDateTime is exclusive. This is so that if a record happens to fall right
        // on the overlap, it's only exported once. DDB doesn't allow us to do AND conditions in a range key query,
        // and the BETWEEN is always inclusive. However, the granularity is down to the millisecond, so we can just
        // use endDateTime minus 1 millisecond.
        long startEpochTime = startDateTime.getMillis();
        long endEpochTime = endDateTime.getMillis() - 1;

        // RangeKeyCondition can be shared across multiple queries.
        RangeKeyCondition rangeKeyCondition = new RangeKeyCondition("uploadedOn").between(startEpochTime,
                endEpochTime);

        // We need to make a separate query for _each_ study in the whitelist. That's just how DDB hash keys work.
        List<Iterable<Item>> recordItemIterList = new ArrayList<>();
        for (String oneStudyId : request.getStudyWhitelist()) {
            Iterable<Item> recordItemIter = ddbQueryHelper.query(ddbRecordStudyUploadedOnIndex, "studyId", oneStudyId,
                    rangeKeyCondition);
            recordItemIterList.add(recordItemIter);
        }

        // Concatenate all the iterables together.
        return new RecordAndStudyListHolder(new RecordIdSource<>(Iterables.concat(recordItemIterList), DYNAMO_ITEM_CONVERTER), bootstrapStudyIdsToQuery(request), getEndDateTime(request));
    }

    /** Get the record ID source from a DDB query. */
    private RecordAndStudyListHolder getDynamoRecordIdSource(BridgeExporterRequest request) {
        Iterable<Item> recordItemIter = ddbQueryHelper.query(ddbRecordUploadDateIndex, "uploadDate",
                request.getDate().toString());
        return new RecordAndStudyListHolder(new RecordIdSource<>(recordItemIter, DYNAMO_ITEM_CONVERTER), bootstrapStudyIdsToQuery(request), getEndDateTime(request));
    }

    /**
     * Get the record ID source from a record override file in S3. We assume the list of record IDs is small enough to
     * reasonably fit in memory.
     */
    private RecordAndStudyListHolder getS3RecordIdSource(BridgeExporterRequest request) throws IOException {
        List<String> recordIdList = s3Helper.readS3FileAsLines(overrideBucket, request.getRecordIdS3Override());
        return new RecordAndStudyListHolder(new RecordIdSource<>(recordIdList, NOOP_CONVERTER), ImmutableMap.of(), DateTime.now());
    }

    private DateTime getEndDateTime(BridgeExporterRequest request) {
        ExportType exportType = request.getExportType();

        if (exportType == ExportType.INSTANT) {
            // set end date time to 1 min ago to avoid clock skew issue for instant export
            return DateTime.now().minusMinutes(1);
        } else {
            return request.getEndDateTime();
        }
    }

    /**
     * Helper method to generate study ids for query
     * @param request
     * @return
     */
    private Map<String, DateTime> bootstrapStudyIdsToQuery(BridgeExporterRequest request) {
        DateTime endDateTime = getEndDateTime(request);
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
        Map<String, DateTime> studyIdsToQuery = new LinkedHashMap<>();

        for (String studyId : studyIdList) {
            Item studyIdItem = ddbExportTimeTable.getItem(STUDY_ID, studyId);
            if (studyIdItem != null && !request.getIgnoreLastExportTime()) {
                studyIdsToQuery.put(studyId, new DateTime(studyIdItem.getLong(LAST_EXPORT_DATE_TIME), timeZone));
            } else {
                // bootstrap startDateTime with the exportType in request
                ExportType exportType = request.getExportType();
                studyIdsToQuery.put(studyId, exportType.getStartDateTime(endDateTime));
            }
        }

        return studyIdsToQuery;
    }

}
