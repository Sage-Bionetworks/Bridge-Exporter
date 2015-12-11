package org.sagebionetworks.bridge.worker;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;

import org.sagebionetworks.bridge.exporter.UploadSchemaKey;

/**
 * Represents a single export table task, such as processing a record into TSV row, or uploading the TSV to a Synapse
 * table.
 */
public class ExportTask {
    private final UploadSchemaKey schemaKey;
    private final Item record;
    private final JsonNode dataJsonNode;

    public ExportTask(UploadSchemaKey schemaKey, Item record, JsonNode dataJsonNode) {
        this.schemaKey = schemaKey;
        this.record = record;
        this.dataJsonNode = dataJsonNode;
    }

    /**
     * Schema key this task represents, used for Health Data and App Version tasks, but not iOS Survey tasks (since iOS
     * Survey tasks don't know the real schema). */
    public UploadSchemaKey getSchemaKey() {
        return schemaKey;
    }

    /** DDB health data record. */
    public Item getRecord() {
        return record;
    }

    /**
     * JSON node representing the data normally found in the DDB health data record. This is used by the iOS Survey
     * Export Handler to transform the iOS Survey into something the Health Data Export Handler can use. The Health
     * Data Export Handler should look at this node before falling back to the record.
     */
    public JsonNode getDataJsonNode() {
        return dataJsonNode;
    }
}
