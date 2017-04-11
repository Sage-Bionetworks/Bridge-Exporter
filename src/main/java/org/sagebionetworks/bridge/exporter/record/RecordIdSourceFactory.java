package org.sagebionetworks.bridge.exporter.record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Resource;

import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.google.common.collect.Iterables;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
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

    // config vars
    private String overrideBucket;

    // Spring helpers
    private DynamoQueryHelper ddbQueryHelper;
    private Index ddbRecordStudyUploadedOnIndex;
    private S3Helper s3Helper;

    /** Config, used to get S3 bucket for record ID override files. */
    @Autowired
    final void setConfig(Config config) {
        overrideBucket = config.get(BridgeExporterUtil.CONFIG_KEY_RECORD_ID_OVERRIDE_BUCKET);
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
    public Iterable<String> getRecordSourceForRequest(BridgeExporterRequest request, DateTime endDateTime, Map<String, DateTime> studyIdsToQuery) throws IOException {
        if (StringUtils.isNotBlank(request.getRecordIdS3Override())) {
            return getS3RecordIdSource(request);
        } else {
            return getDynamoRecordIdSourceGeneral(endDateTime, studyIdsToQuery);
        }
    }

    /**
     * Helper method to get ddb records
     * @return
     */
    private Iterable<String> getDynamoRecordIdSourceGeneral(DateTime endDateTime, Map<String, DateTime> studyIdsToQuery) {
        // proceed
        Iterable<Item> recordItemIter;

        // We need to make a separate query for _each_ study in the whitelist. That's just how DDB hash keys work.
        List<Iterable<Item>> recordItemIterList = new ArrayList<>();
        for (Map.Entry<String, DateTime> oneStudyIdAndDateTime : studyIdsToQuery.entrySet()) {
            Iterable<Item> recordItemIterTemp = ddbQueryHelper.query(ddbRecordStudyUploadedOnIndex, "studyId", oneStudyIdAndDateTime.getKey(),
                    new RangeKeyCondition("uploadedOn").between(oneStudyIdAndDateTime.getValue().getMillis(),
                            endDateTime.getMillis()));

            recordItemIterList.add(recordItemIterTemp);
        }

        recordItemIter = Iterables.concat(recordItemIterList);

        return new RecordIdSource<>(recordItemIter, DYNAMO_ITEM_CONVERTER);
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
