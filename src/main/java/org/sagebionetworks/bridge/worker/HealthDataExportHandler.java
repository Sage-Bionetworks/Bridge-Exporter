package org.sagebionetworks.bridge.worker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;

import org.sagebionetworks.bridge.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.exporter.PhoneAppVersionInfo;
import org.sagebionetworks.bridge.exporter.UploadSchema;
import org.sagebionetworks.bridge.exporter.UploadSchemaKey;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.util.BridgeExporterUtil;

/** Synapse export worker for health data tables. */
public class HealthDataExportHandler extends SynapseExportHandler {
    // Configured externally
    private UploadSchemaKey schemaKey;

    // Internal state
    private UploadSchema schema;

    /** Schema key corresponding to this health data record table. Configured externally. */
    public UploadSchemaKey getSchemaKey() {
        return schemaKey;
    }

    public void setSchemaKey(UploadSchemaKey schemaKey) {
        this.schemaKey = schemaKey;
    }

    @Override
    protected void initSchemas() throws SchemaNotFoundException {
        schema = getManager().getSchemaHelper().getSchema(schemaKey);
    }

    @Override
    protected String getDdbTableName() {
        return "SynapseTables";
    }

    @Override
    protected String getDdbTableKeyName() {
        return "schemaKey";
    }

    @Override
    protected List<ColumnModel> getSynapseTableColumnList() {
        List<ColumnModel> columnList = new ArrayList<>();

        // common columns
        ColumnModel recordIdColumn = new ColumnModel();
        recordIdColumn.setName("recordId");
        recordIdColumn.setColumnType(ColumnType.STRING);
        recordIdColumn.setMaximumSize(36L);
        columnList.add(recordIdColumn);

        ColumnModel healthCodeColumn = new ColumnModel();
        healthCodeColumn.setName("healthCode");
        healthCodeColumn.setColumnType(ColumnType.STRING);
        healthCodeColumn.setMaximumSize(36L);
        columnList.add(healthCodeColumn);

        ColumnModel externalIdColumn = new ColumnModel();
        externalIdColumn.setName("externalId");
        externalIdColumn.setColumnType(ColumnType.STRING);
        externalIdColumn.setMaximumSize(128L);
        columnList.add(externalIdColumn);

        // NOTE: ColumnType.DATE is actually a timestamp. There is no calendar date type.
        ColumnModel uploadDateColumn = new ColumnModel();
        uploadDateColumn.setName("uploadDate");
        uploadDateColumn.setColumnType(ColumnType.STRING);
        uploadDateColumn.setMaximumSize(10L);
        columnList.add(uploadDateColumn);

        ColumnModel createdOnColumn = new ColumnModel();
        createdOnColumn.setName("createdOn");
        createdOnColumn.setColumnType(ColumnType.DATE);
        columnList.add(createdOnColumn);

        ColumnModel appVersionColumn = new ColumnModel();
        appVersionColumn.setName("appVersion");
        appVersionColumn.setColumnType(ColumnType.STRING);
        appVersionColumn.setMaximumSize(48L);
        columnList.add(appVersionColumn);

        ColumnModel phoneInfoColumn = new ColumnModel();
        phoneInfoColumn.setName("phoneInfo");
        phoneInfoColumn.setColumnType(ColumnType.STRING);
        phoneInfoColumn.setMaximumSize(48L);
        columnList.add(phoneInfoColumn);

        // schema specific columns
        List<String> fieldNameList = schema.getFieldNameList();
        Map<String, String> fieldTypeMap = schema.getFieldTypeMap();
        for (String oneFieldName : fieldNameList) {
            String bridgeType = fieldTypeMap.get(oneFieldName);

            ColumnType synapseType = SynapseHelper.BRIDGE_TYPE_TO_SYNAPSE_TYPE.get(bridgeType);
            if (synapseType == null) {
                System.out.println("[ERROR] No Synapse type found for Bridge type " + bridgeType);
                synapseType = ColumnType.STRING;
            }

            // hack to cover legacy schemas pre-1k char limit on strings. See comments on
            // shouldConvertFreeformTextToAttachment() for more details.
            if (BridgeExporterUtil.shouldConvertFreeformTextToAttachment(schemaKey, oneFieldName)) {
                synapseType = ColumnType.FILEHANDLEID;
            }

            ColumnModel oneColumn = new ColumnModel();
            oneColumn.setName(oneFieldName);
            oneColumn.setColumnType(synapseType);

            if (synapseType == ColumnType.STRING) {
                Integer maxLength = SynapseHelper.BRIDGE_TYPE_TO_MAX_LENGTH.get(bridgeType);
                if (maxLength == null) {
                    System.out.println("[ERROR] No max length found for Bridge type " + bridgeType);
                    maxLength = SynapseHelper.DEFAULT_MAX_LENGTH;
                }
                oneColumn.setMaximumSize(Long.valueOf(maxLength));
            }

            columnList.add(oneColumn);
        }

        return columnList;
    }

    @Override
    protected String getSynapseTableName() {
        return schemaKey.toString();
    }

    @Override
    protected List<String> getTsvHeaderList() {
        List<String> tsvFieldNameList = new ArrayList<>();

        // common fields
        tsvFieldNameList.add("recordId");
        tsvFieldNameList.add("healthCode");
        tsvFieldNameList.add("externalId");
        tsvFieldNameList.add("uploadDate");
        tsvFieldNameList.add("createdOn");
        tsvFieldNameList.add("appVersion");
        tsvFieldNameList.add("phoneInfo");

        // schema specific fields
        tsvFieldNameList.addAll(schema.getFieldNameList());

        return tsvFieldNameList;
    }

    @Override
    protected List<String> getTsvRowValueList(ExportTask task) throws BridgeExporterException {
        Item record = task.getRecord();
        if (record == null) {
            throw new BridgeExporterException("Null record for HealthDataExportWorker");
        }
        String recordId = record.getString("id");

        // Extract data JSON. Try task.getDataJsonNode() first. If not, fall back to record.getString("data").
        JsonNode dataJson = task.getDataJsonNode();
        if (dataJson == null || dataJson.isNull()) {
            try {
                dataJson = BridgeExporterUtil.JSON_MAPPER.readTree(record.getString("data"));
            } catch (IOException ex) {
                throw new BridgeExporterException("Error parsing JSON: " + ex.getMessage(), ex);
            }
        }
        if (dataJson == null || dataJson.isNull()) {
            throw new BridgeExporterException("Null data JSON node for HealthDataExportWorker");
        }

        // get phone and app info
        PhoneAppVersionInfo phoneAppVersionInfo = PhoneAppVersionInfo.fromRecord(record);
        String appVersion = phoneAppVersionInfo.getAppVersion();
        String phoneInfo = phoneAppVersionInfo.getPhoneInfo();

        List<String> rowValueList = new ArrayList<>();

        // common values
        rowValueList.add(recordId);
        rowValueList.add(record.getString("healthCode"));
        rowValueList.add(BridgeExporterUtil.getDdbStringRemoveTabsAndTrim(record, "userExternalId", 128, recordId));
        rowValueList.add(getManager().getTodaysDateString());

        // createdOn as a long epoch millis
        rowValueList.add(String.valueOf(record.getLong("createdOn")));

        rowValueList.add(appVersion);
        rowValueList.add(phoneInfo);

        // schema-specific columns
        List<String> fieldNameList = schema.getFieldNameList();
        Map<String, String> fieldTypeMap = schema.getFieldTypeMap();
        for (String oneFieldName : fieldNameList) {
            String bridgeType = fieldTypeMap.get(oneFieldName);
            JsonNode valueNode = dataJson.get(oneFieldName);

            if (BridgeExporterUtil.shouldConvertFreeformTextToAttachment(schemaKey, oneFieldName)) {
                // special hack, see comments on shouldConvertFreeformTextToAttachment()
                bridgeType = "attachment_blob";
                if (valueNode != null && !valueNode.isNull() && valueNode.isTextual()) {
                    try {
                        String attachmentId = getManager().getExportHelper().uploadFreeformTextAsAttachment(recordId,
                                valueNode.textValue());
                        valueNode = new TextNode(attachmentId);
                    } catch (AmazonClientException | IOException ex) {
                        System.out.println("[ERROR] Error uploading freeform text as attachment for record ID "
                                + recordId + ", field " + oneFieldName + ": " + ex.getMessage());
                        valueNode = null;
                    }
                } else {
                    valueNode = null;
                }
            }

            String value = getManager().getSynapseHelper().serializeToSynapseType(getStudyId(), recordId, oneFieldName,
                    bridgeType, valueNode);
            rowValueList.add(value);
        }

        return rowValueList;
    }
}
