package org.sagebionetworks.bridge.exporter;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import org.sagebionetworks.repo.model.table.ColumnType;

import org.sagebionetworks.bridge.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.util.BridgeExporterUtil;

/**
 * This class contains utility methods that call multiple backend services that doesn't really fit into any of the
 * other helpers.
 */
public class ExportHelper {
    private BridgeExporterConfig config;
    private DynamoDB ddbClient;
    private S3Helper s3Helper;

    /** Bridge Exporter configuration. Configured externally. */
    public BridgeExporterConfig getConfig() {
        return config;
    }

    public void setConfig(BridgeExporterConfig config) {
        this.config = config;
    }

    /** DynamoDB client. Configured externally. */
    public DynamoDB getDdbClient() {
        return ddbClient;
    }

    public void setDdbClient(DynamoDB ddbClient) {
        this.ddbClient = ddbClient;
    }

    /** S3 helper. Configured externally. */
    public S3Helper getS3Helper() {
        return s3Helper;
    }

    public void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    public JsonNode convertSurveyRecordToHealthDataJsonNode(Item record, UploadSchema surveySchema) throws BridgeExporterException {
        if (record == null) {
            throw new BridgeExporterException("Null record for HealthDataExportWorker");
        }
        String recordId = record.getString("id");

        // get data node and item
        JsonNode oldDataJson;
        try {
            oldDataJson = BridgeExporterUtil.JSON_MAPPER.readTree(record.getString("data"));
        } catch (IOException ex) {
            throw new BridgeExporterException("Error parsing JSON data: " + ex.getMessage(), ex);
        }
        if (oldDataJson == null || oldDataJson.isNull()) {
            throw new BridgeExporterException("No JSON data");
        }

        // download answers from S3 attachments
        JsonNode answerLinkNode = oldDataJson.get("answers");
        if (answerLinkNode == null || answerLinkNode.isNull()) {
            throw new BridgeExporterException("No answer link in survey data");
        }
        String answerLink = answerLinkNode.textValue();
        String answerText;
        try {
            answerText = s3Helper.readS3FileAsString(config.getBridgeAttachmentsBucket(), answerLink);
        } catch (AmazonClientException | IOException ex) {
            throw new BridgeExporterException("Error getting survey answers from S3 file " + answerLink + ": "
                    + ex.getMessage(), ex);
        }

        JsonNode answerArrayNode;
        try {
            answerArrayNode = BridgeExporterUtil.JSON_MAPPER.readTree(answerText);
        } catch (IOException ex) {
            throw new BridgeExporterException("Error parsing JSON survey answers from S3 file " + answerLink + ": "
                    + ex.getMessage(), ex);
        }
        if (answerArrayNode == null || answerArrayNode.isNull()) {
            throw new BridgeExporterException("Survey with no answers from S3 file " + answerLink);
        }

        // get schema and field type map, so we can process attachments
        Map<String, String> surveyFieldTypeMap = surveySchema.getFieldTypeMap();

        // copy fields to "non-survey" format
        ObjectNode convertedSurveyNode = BridgeExporterUtil.JSON_MAPPER.createObjectNode();
        int numAnswers = answerArrayNode.size();
        for (int i = 0; i < numAnswers; i++) {
            JsonNode oneAnswerNode = answerArrayNode.get(i);
            if (oneAnswerNode == null || oneAnswerNode.isNull()) {
                System.out.println("[ERROR] Survey record ID " + recordId + " answer " + i + " has no value");
                continue;
            }

            // question name ("item")
            JsonNode answerItemNode = oneAnswerNode.get("item");
            if (answerItemNode == null || answerItemNode.isNull()) {
                System.out.println("[ERROR] Survey record ID " + recordId + " answer " + i
                        + " has no question name (item)");
                continue;
            }
            String answerItem = answerItemNode.textValue();
            if (Strings.isNullOrEmpty(answerItem)) {
                System.out.println("[ERROR] Survey record ID " + recordId + " answer " + i
                        + " has null or empty question name (item)");
                continue;
            }

            // question type
            JsonNode questionTypeNameNode = oneAnswerNode.get("questionTypeName");
            if (questionTypeNameNode == null || questionTypeNameNode.isNull()) {
                // fall back to questionType
                questionTypeNameNode = oneAnswerNode.get("questionType");
            }
            if (questionTypeNameNode == null || questionTypeNameNode.isNull()) {
                System.out.println("[ERROR] Survey record ID " + recordId + " answer " + i + " has no question type");
                continue;
            }
            String questionTypeName = questionTypeNameNode.textValue();
            if (Strings.isNullOrEmpty(questionTypeName)) {
                System.out.println("[ERROR] Survey record ID " + recordId + " answer " + i
                        + " has null or empty question type");
                continue;
            }

            // answer
            // TODO: Hey, this should really be a Map<String, String>, not a big switch statement
            JsonNode answerAnswerNode = null;
            switch (questionTypeName) {
                case "Boolean":
                    answerAnswerNode = oneAnswerNode.get("booleanAnswer");
                    break;
                case "Date":
                    answerAnswerNode = oneAnswerNode.get("dateAnswer");
                    break;
                case "Decimal":
                case "Integer":
                    answerAnswerNode = oneAnswerNode.get("numericAnswer");
                    break;
                case "MultipleChoice":
                case "SingleChoice":
                    answerAnswerNode = oneAnswerNode.get("choiceAnswers");
                    break;
                case "None":
                case "Scale":
                    // yes, None really gets the answer from scaleAnswer
                    answerAnswerNode = oneAnswerNode.get("scaleAnswer");
                    break;
                case "Text":
                    answerAnswerNode = oneAnswerNode.get("textAnswer");
                    break;
                case "TimeInterval":
                    answerAnswerNode = oneAnswerNode.get("intervalAnswer");
                    break;
                case "TimeOfDay":
                    answerAnswerNode = oneAnswerNode.get("dateComponentsAnswer");
                    break;
                default:
                    System.out.println("[ERROR] Survey record ID " + recordId + " answer " + i
                            + " has unknown question type " + questionTypeName);
                    break;
            }

            if (answerAnswerNode != null && !answerAnswerNode.isNull()) {
                // handle attachment types (file handle types)
                String bridgeType = surveyFieldTypeMap.get(answerItem);
                ColumnType synapseType = SynapseHelper.BRIDGE_TYPE_TO_SYNAPSE_TYPE.get(bridgeType);
                if (synapseType == ColumnType.FILEHANDLEID) {
                    String attachmentId = null;
                    String answer = answerAnswerNode.toString();
                    try {
                        attachmentId = uploadFreeformTextAsAttachment(recordId, answer);
                    } catch (AmazonClientException | IOException ex) {
                        System.out.println("[ERROR] Error uploading freeform text as attachment for record ID "
                                + recordId + ", survey item " + answerItem + ": " + ex.getMessage());
                    }

                    convertedSurveyNode.put(answerItem, attachmentId);
                } else {
                    convertedSurveyNode.set(answerItem, answerAnswerNode);
                }
            }

            // if there's a unit, add it as well
            JsonNode unitNode = oneAnswerNode.get("unit");
            if (unitNode != null && !unitNode.isNull()) {
                convertedSurveyNode.set(answerItem + "_unit", unitNode);
            }
        }

        return convertedSurveyNode;
    }

    public String uploadFreeformTextAsAttachment(String recordId, String text)
            throws AmazonClientException, IOException {
        // write to health data attachments table to reserve guid
        String attachmentId = UUID.randomUUID().toString();
        Item attachment = new Item();
        attachment.withString("id", attachmentId);
        attachment.withString("recordId", recordId);

        Table attachmentsTable = ddbClient.getTable(config.getBridgeDataDdbPrefix() + "HealthDataAttachment");
        attachmentsTable.putItem(attachment);

        // upload to S3
        s3Helper.writeBytesToS3(config.getBridgeAttachmentsBucket(), attachmentId, text.getBytes(Charsets.UTF_8));
        return attachmentId;
    }
}
