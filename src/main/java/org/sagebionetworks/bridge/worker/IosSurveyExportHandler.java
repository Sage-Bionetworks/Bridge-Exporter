package org.sagebionetworks.bridge.worker;

import java.io.IOException;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;

import org.sagebionetworks.bridge.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exceptions.SchemaNotFoundException;
import org.sagebionetworks.bridge.exporter.UploadSchema;
import org.sagebionetworks.bridge.exporter.UploadSchemaKey;
import org.sagebionetworks.bridge.util.BridgeExporterUtil;

/**
 * This class is responsible for converting the raw survey answers into a form the Synapse Export Worker can consume.
 * Because this doesn't create Synapse tables, rather only converting the data and forwarding it to other worker
 * threads, this worker has no init and no cleanup, only the worker loop.
 */
// TODO: move this logic to the Bridge server
public class IosSurveyExportHandler extends ExportHandler {
    private int errorCount = 0;
    private int surveyCount = 0;

    @Override
    public void init() throws BridgeExporterException, SchemaNotFoundException {
        // noop
    }

    @Override
    public void handle(ExportTask task) {
        Item record = task.getRecord();
        String recordId = record != null ? record.getString("id") : null;

        try {
            processRecordAsSurvey(task);
            surveyCount++;
        } catch (SchemaNotFoundException ex) {
            errorCount++;
            System.out.println("[ERROR] Schema not found for record " + recordId + " for study " + getStudyId()
                    + ": " + ex.getMessage());
        } catch (BridgeExporterException ex) {
            errorCount++;
            System.out.println("[ERROR] Error processing record " + recordId + " for survey handler for study "
                    + getStudyId() + ": " + ex.getMessage());
        } catch (RuntimeException ex) {
            errorCount++;
            System.out.println("[ERROR] RuntimeException processing record " + recordId
                    + " for survey handler for study " + getStudyId() + ": " + ex.getMessage());
            ex.printStackTrace(System.out);
        }
    }

    private void processRecordAsSurvey(ExportTask task) throws BridgeExporterException, SchemaNotFoundException {
        // validate record
        Item record = task.getRecord();
        if (record == null) {
            throw new BridgeExporterException("Null record for HealthDataExportWorker");
        }

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
        JsonNode itemNode = oldDataJson.get("item");
        if (itemNode == null || itemNode.isNull()) {
            throw new BridgeExporterException("No item field in survey data");
        }
        String item = itemNode.textValue();
        if (Strings.isNullOrEmpty(item)) {
            throw new BridgeExporterException("Null or empty item field in survey data");
        }

        // what schema should we forward this to?
        String schemaId = item;
        int schemaRev = 1;
        UploadSchemaKey surveySchemaKey = new UploadSchemaKey(getStudyId(), schemaId, schemaRev);

        // get schema and field type map, so we can process attachments
        UploadSchema surveySchema = getManager().getSchemaHelper().getSchema(surveySchemaKey);

        // convert to health data node
        JsonNode convertedSurveyNode = getManager().getExportHelper().convertSurveyRecordToHealthDataJsonNode(record,
                surveySchema);

        ExportTask convertedTask = new ExportTask(surveySchemaKey, record, convertedSurveyNode);
        getManager().addHealthDataExportTask(surveySchemaKey, convertedTask);
    }

    @Override
    public void endOfStream() {
        // noop
    }

    @Override
    public void reportMetrics() {
        if (surveyCount > 0) {
            System.out.println("[METRICS] surveyWorker[" + getStudyId() + "].surveyCount: " + surveyCount);
        }
        if (errorCount > 0) {
            System.out.println("[METRICS] surveyWorker[" + getStudyId() + "].errorCount: " + errorCount);
        }
    }
}
