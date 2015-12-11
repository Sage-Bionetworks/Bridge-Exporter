package org.sagebionetworks.bridge.exporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

// This is different from the class in BridgePF, but should eventually be merged with it.
public class UploadSchema {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final UploadSchemaKey key;
    private final List<String> fieldNameList;
    private final Map<String, String> fieldTypeMap;

    public UploadSchema(UploadSchemaKey key, List<String> fieldNameList, Map<String, String> fieldTypeMap) {
        this.key = key;
        this.fieldNameList = ImmutableList.copyOf(fieldNameList);
        this.fieldTypeMap = ImmutableMap.copyOf(fieldTypeMap);
    }

    public static UploadSchema fromDdbItem(UploadSchemaKey key, Item ddbUploadSchema) throws IOException {
        List<String> fieldNameList = new ArrayList<>();
        Map<String, String> fieldTypeMap = new HashMap<>();

        JsonNode fieldDefList = JSON_MAPPER.readTree(ddbUploadSchema.getString("fieldDefinitions"));
        for (JsonNode oneFieldDef : fieldDefList) {
            String name = oneFieldDef.get("name").textValue();
            String bridgeType = oneFieldDef.get("type").textValue().toLowerCase();

            fieldNameList.add(name);
            fieldTypeMap.put(name, bridgeType);
        }

        return new UploadSchema(key, fieldNameList, fieldTypeMap);
    }

    public UploadSchemaKey getKey() {
        return key;
    }

    // preserves order of fields, such as for table columns
    public List<String> getFieldNameList() {
        return fieldNameList;
    }

    public Map<String, String> getFieldTypeMap() {
        return fieldTypeMap;
    }
}
