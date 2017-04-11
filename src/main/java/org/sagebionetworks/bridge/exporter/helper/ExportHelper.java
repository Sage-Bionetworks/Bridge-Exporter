package org.sagebionetworks.bridge.exporter.helper;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.exporter.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exporter.record.ExportType;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.synapse.SynapseHelper;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.model.UploadFieldDefinition;
import org.sagebionetworks.bridge.rest.model.UploadSchema;
import org.sagebionetworks.bridge.s3.S3Helper;

/**
 * This class contains utility methods that call multiple backend services that doesn't really fit into any of the
 * other helpers.
 */
@Component
public class ExportHelper {
    private static final Logger LOG = LoggerFactory.getLogger(ExportHelper.class);

    private static final Map<String, String> SURVEY_TYPE_TO_ANSWER_KEY_MAP = ImmutableMap.<String, String>builder()
            .put("Boolean", "booleanAnswer")
            .put("Date", "dateAnswer")
            .put("Decimal", "numericAnswer")
            .put("Integer", "numericAnswer")
            .put("MultipleChoice", "choiceAnswers")
                    // yes, None really gets the answer from scaleAnswer
            .put("None", "scaleAnswer")
            .put("Scale", "scaleAnswer")
            .put("SingleChoice", "choiceAnswers")
            .put("Text", "textAnswer")
            .put("TimeInterval", "intervalAnswer")
            .put("TimeOfDay", "dateComponentsAnswer")
            .build();

    // config attributes
    private String attachmentBucket;
    private DateTimeZone timeZone;

    // Spring helpers
    private Table ddbAttachmentTable;
    private S3Helper s3Helper;

    /** Config, used to get the S3 attachments bucket. */
    @Autowired
    public final void setConfig(Config config) {
        this.attachmentBucket = config.get(BridgeExporterUtil.CONFIG_KEY_ATTACHMENT_S3_BUCKET);
        timeZone = DateTimeZone.forID(config.get(BridgeExporterUtil.CONFIG_KEY_TIME_ZONE_NAME));
    }

    /** DDB Attachments table, for uploading and downloading attachments. */
    @Autowired
    public final void setDdbAttachmentTable(Table ddbAttachmentTable) {
        this.ddbAttachmentTable = ddbAttachmentTable;
    }

    /** S3 helper, for uploading and downloading attachments. */
    @Autowired
    public final void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    /**
     * Helper method to convert legacy surveys to a Synapse record.
     *
     * @param recordId
     *         record ID, for logging purposes
     * @param oldDataJson
     *         the old legacy survey
     * @param surveySchema
     *         survey schema to convert with
     * @return JSON node representing the converted Synapse record
     * @throws BridgeExporterException
     *         if there's an error parsing the survey answers that can't be recovered
     * @throws IOException
     *         error parsing JSON answers
     */
    public JsonNode convertSurveyRecordToHealthDataJsonNode(String recordId, JsonNode oldDataJson,
            UploadSchema surveySchema) throws BridgeExporterException, IOException {
        // download answers from S3 attachments
        JsonNode answerLinkNode = oldDataJson.get("answers");
        if (answerLinkNode == null || answerLinkNode.isNull()) {
            throw new BridgeExporterException("No answer link in survey data");
        }
        String answerLink = answerLinkNode.textValue();
        if (StringUtils.isBlank(answerLink)) {
            throw new BridgeExporterException("Answer link in survey data must be specified");
        }
        String answerText = s3Helper.readS3FileAsString(attachmentBucket, answerLink);

        JsonNode answerArrayNode;
        try {
            answerArrayNode = DefaultObjectMapper.INSTANCE.readTree(answerText);
        } catch (IOException ex) {
            throw new BridgeExporterException("Error parsing JSON survey answers from S3 file " + answerLink + ": " +
                    ex.getMessage(), ex);
        }
        if (answerArrayNode == null || answerArrayNode.isNull()) {
            throw new BridgeExporterException("Survey with no answers from S3 file " + answerLink);
        }

        // get schema and field type map, so we can process attachments
        Map<String, UploadFieldDefinition> surveyFieldDefMap = BridgeExporterUtil.getFieldDefMapFromSchema(
                surveySchema);

        // copy fields to "non-survey" format
        ObjectNode convertedSurveyNode = DefaultObjectMapper.INSTANCE.createObjectNode();
        int numAnswers = answerArrayNode.size();
        for (int i = 0; i < numAnswers; i++) {
            JsonNode oneAnswerNode = answerArrayNode.get(i);
            if (oneAnswerNode.isNull()) {
                LOG.error("Survey record ID " + recordId + " answer " + i + " has no value");
                continue;
            }

            // question name ("item")
            JsonNode answerItemNode = oneAnswerNode.get("item");
            if (answerItemNode == null || answerItemNode.isNull()) {
                LOG.error("Survey record ID " + recordId + " answer " + i + " has no question name (item)");
                continue;
            }
            String answerItem = answerItemNode.textValue();
            if (StringUtils.isBlank(answerItem)) {
                LOG.error("Survey record ID " + recordId + " answer " + i + " has blank question name (item)");
                continue;
            }

            // question type
            JsonNode questionTypeNameNode = oneAnswerNode.get("questionTypeName");
            if (questionTypeNameNode == null || questionTypeNameNode.isNull()) {
                // fall back to questionType
                questionTypeNameNode = oneAnswerNode.get("questionType");
            }
            if (questionTypeNameNode == null || questionTypeNameNode.isNull()) {
                LOG.error("Survey record ID " + recordId + " item " + answerItem + " has no question type");
                continue;
            }
            String questionTypeName = questionTypeNameNode.textValue();
            if (StringUtils.isBlank(questionTypeName)) {
                LOG.error("Survey record ID " + recordId + " item " + answerItem + " has blank question type");
                continue;
            }

            // answer
            String answerKey = SURVEY_TYPE_TO_ANSWER_KEY_MAP.get(questionTypeName);
            if (answerKey == null) {
                LOG.error("Survey record ID " + recordId + " item " + answerItem + " has unknown question type " +
                        questionTypeName);
                continue;
            }

            JsonNode answerAnswerNode = oneAnswerNode.get(answerKey);
            if (answerAnswerNode != null && !answerAnswerNode.isNull()) {
                // handle attachment types (file handle types)
                UploadFieldDefinition fieldDef = surveyFieldDefMap.get(answerItem);
                if (fieldDef == null) {
                    // Not in the schema. Log an error and skip.
                    LOG.error("Survey record ID " + recordId + " item " + answerItem + " not found in schema");
                    continue;
                }

                ColumnType synapseType = SynapseHelper.BRIDGE_TYPE_TO_SYNAPSE_TYPE.get(fieldDef.getType());
                if (synapseType == ColumnType.FILEHANDLEID) {
                    String attachmentId = uploadFreeformTextAsAttachment(recordId, answerAnswerNode.toString());
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

    /**
     * Uploads the given freeform text as an attachment associated with the given record. This updates both the
     * attachments DDB table and the attachments S3 bucket.
     *
     * @param recordId
     *         record to associate the attachment with
     * @param text
     *         text of the attachment
     * @return attachment ID
     * @throws IOException
     *         if uploading to S3 fails
     */
    public String uploadFreeformTextAsAttachment(String recordId, String text) throws IOException {
        // write to health data attachments table to reserve guid
        String attachmentId = UUID.randomUUID().toString();
        Item attachment = new Item();
        attachment.withString("id", attachmentId);
        attachment.withString("recordId", recordId);
        ddbAttachmentTable.putItem(attachment);

        // upload to S3
        s3Helper.writeBytesToS3(attachmentBucket, attachmentId, text.getBytes(Charsets.UTF_8));
        return attachmentId;
    }

    /**
     * Helper method to get end date time from sqs request
     * @param request
     * @return
     */
    public DateTime getEndDateTime(BridgeExporterRequest request) {
        ExportType exportType = request.getExportType();

        if (exportType == ExportType.INSTANT) {
            // set end date time to 1 min ago to avoid clock skew issue for instant export
            return DateTime.now().minusMinutes(1);
        } else {
            return request.getEndDateTime();
        }
    }
}
