package org.sagebionetworks.bridge.exporter;

import java.util.Iterator;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;

/** Gets record IDs from the Health Data Record Dynamo DB table. */
public class DynamoRecordIdSource extends RecordIdSource {
    // Configured externally.
    private BridgeExporterConfig config;
    private DynamoDB ddbClient;
    private String uploadDateString;

    // Internal state.
    private Iterator<Item> recordKeyIter;

    /** Bridge Exporter configuration. Configured externally. */
    public BridgeExporterConfig getConfig() {
        return config;
    }

    public void setConfig(BridgeExporterConfig config) {
        this.config = config;
    }

    /** Dynamo DB client. Configured externally. */
    public DynamoDB getDdbClient() {
        return ddbClient;
    }

    public void setDdbClient(DynamoDB ddbClient) {
        this.ddbClient = ddbClient;
    }

    /** Upload date in YYYY-MM-DD format. Configured externally. */
    public String getUploadDateString() {
        return uploadDateString;
    }

    public void setUploadDateString(String uploadDateString) {
        this.uploadDateString = uploadDateString;
    }

    @Override
    public void init() {
        // The index only projects the table keys, so we can only get the record ID from this index. We use this ID to
        // re-query DDB to get the full record.
        Table recordTable = ddbClient.getTable(config.getBridgeDataDdbPrefix() + "HealthDataRecord3");
        Index recordTableUploadDateIndex = recordTable.getIndex("uploadDate-index");
        Iterable<Item> recordKeyIterable = recordTableUploadDateIndex.query("uploadDate", uploadDateString);
        recordKeyIter = recordKeyIterable.iterator();
    }

    @Override
    public boolean hasNext() {
        return recordKeyIter.hasNext();
    }

    @Override
    public String next() {
        return recordKeyIter.next().getString("id");
    }
}
