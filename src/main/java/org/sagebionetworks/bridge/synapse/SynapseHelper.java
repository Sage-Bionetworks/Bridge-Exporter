package org.sagebionetworks.bridge.synapse;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.jcabi.aspects.RetryOnFailure;
import org.joda.time.DateTime;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.AccessControlList;
import org.sagebionetworks.repo.model.file.FileHandle;
import org.sagebionetworks.repo.model.table.AppendableRowSet;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.sagebionetworks.repo.model.table.CsvTableDescriptor;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.sagebionetworks.repo.model.table.UploadToTableResult;

import org.sagebionetworks.bridge.exceptions.BridgeExporterException;
import org.sagebionetworks.bridge.exporter.BridgeExporterConfig;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.util.BridgeExporterUtil;

public class SynapseHelper {
    private static final long APPEND_TIMEOUT_MILLISECONDS = 30 * 1000;
    private static final int ASYNC_UPLOAD_TIMEOUT_SECONDS = 300;
    public static final int DEFAULT_MAX_LENGTH = 100;

    public static final Map<String, ColumnType> BRIDGE_TYPE_TO_SYNAPSE_TYPE =
            ImmutableMap.<String, ColumnType>builder()
                    .put("attachment_blob", ColumnType.FILEHANDLEID)
                    .put("attachment_csv", ColumnType.FILEHANDLEID)
                    .put("attachment_json_blob", ColumnType.FILEHANDLEID)
                    .put("attachment_json_table", ColumnType.FILEHANDLEID)
                    .put("boolean", ColumnType.BOOLEAN)
                    .put("calendar_date", ColumnType.STRING)
                    .put("float", ColumnType.DOUBLE)
                    .put("inline_json_blob", ColumnType.STRING)
                    .put("int", ColumnType.INTEGER)
                    .put("string", ColumnType.STRING)
                    .put("timestamp", ColumnType.DATE)
                    .build();

    public static final Map<String, Integer> BRIDGE_TYPE_TO_MAX_LENGTH = ImmutableMap.<String, Integer>builder()
            .put("calendar_date", 10)
            .put("inline_json_blob", 100)
            .put("string", 100)
            .build();

    private BridgeExporterConfig bridgeExporterConfig;
    private S3Helper s3Helper;
    private SynapseClient synapseClient;

    public void setBridgeExporterConfig(BridgeExporterConfig bridgeExporterConfig) {
        this.bridgeExporterConfig = bridgeExporterConfig;
    }

    public void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    public void setSynapseClient(SynapseClient synapseClient) {
        this.synapseClient = synapseClient;
    }

    public String serializeToSynapseType(String studyId, String recordId, String fieldName, String bridgeType,
            JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }

        ColumnType synapseType = BRIDGE_TYPE_TO_SYNAPSE_TYPE.get(bridgeType);
        if (synapseType == null) {
            System.out.println("[ERROR] No Synapse type found for Bridge type " + bridgeType + ", record ID "
                    + recordId);
            synapseType = ColumnType.STRING;
        }

        switch (synapseType) {
            case BOOLEAN:
                if (node.isBoolean()) {
                    return String.valueOf(node.booleanValue());
                }
                return null;
            case DATE:
                // date is a long epoch millis
                if (node.isTextual()) {
                    try {
                        DateTime dateTime = DateTime.parse(node.textValue());
                        return String.valueOf(dateTime.getMillis());
                    } catch (RuntimeException ex) {
                        // throw out malformatted dates
                        return null;
                    }
                } else if (node.isNumber()) {
                    return String.valueOf(node.longValue());
                }
                return null;
            case DOUBLE:
                // double only has double precision, not BigDecimal precision
                if (node.isNumber()) {
                    return String.valueOf(node.doubleValue());
                }
                return null;
            case FILEHANDLEID:
                // file handles are text nodes, where the text is the attachment ID (which is the S3 Key)
                if (node.isTextual()) {
                    String s3Key = node.textValue();
                    try {
                        return uploadFromS3ToSynapseFileHandle(studyId, fieldName, s3Key);
                    } catch (IOException | SynapseException ex) {
                        System.out.println("[ERROR] Error uploading attachment to Synapse for record ID " + recordId +
                                ", s3Key " + s3Key + ": " + ex.getMessage());
                        ex.printStackTrace(System.out);
                        return null;
                    }
                }
                return null;
            case INTEGER:
                // int includes long
                if (node.isNumber()) {
                    return String.valueOf(node.longValue());
                }
                return null;
            case STRING:
                // String includes calendar_date (as JSON string) and inline_json_blob (arbitrary JSON)
                String nodeValue;
                if ("inline_json_blob".equalsIgnoreCase(bridgeType)) {
                    nodeValue = node.toString();
                } else {
                    nodeValue = node.asText();
                }

                Integer maxLength = BRIDGE_TYPE_TO_MAX_LENGTH.get(bridgeType);
                if (maxLength == null) {
                    System.out.println("[ERROR] No max length found for Bridge type " + bridgeType);
                    maxLength = DEFAULT_MAX_LENGTH;
                }
                String filtered = BridgeExporterUtil.removeTabs(nodeValue);
                String trimmed =  BridgeExporterUtil.trimToLengthAndWarn(filtered, maxLength, recordId);
                return trimmed;
            default:
                System.out.println("[ERROR] Unexpected Synapse Type " + String.valueOf(synapseType) + " for record ID "
                        + recordId);
                return null;
        }
    }

    private String uploadFromS3ToSynapseFileHandle(String studyId, String fieldName, String s3Key) throws IOException,
            SynapseException {
        // create temp file using the field name and s3Key as the prefix and the default suffix (null)
        File tempFile = File.createTempFile(fieldName + "-" + s3Key, null);

        try {
            // download from S3
            // TODO: update S3Helper to stream directly to a file instead of holding it in memory first
            byte[] fileBytes = s3Helper.readS3FileAsBytes(bridgeExporterConfig.getBridgeAttachmentsBucket(), s3Key);
            Files.write(fileBytes, tempFile);

            // upload to Synapse
            FileHandle synapseFileHandle = createFileHandleWithRetry(tempFile, "application/octet-stream",
                    bridgeExporterConfig.getProjectIdsByStudy().get(studyId));
            return synapseFileHandle.getId();
        } finally {
            // delete temp file
            tempFile.delete();
        }
    }

    public long uploadTsvFileToTable(String projectId, String tableId, File file) throws BridgeExporterException {
        // upload file to synapse as a file handle
        FileHandle tableFileHandle;
        try {
            tableFileHandle = createFileHandleWithRetry(file, "text/tab-separated-values", projectId);
        } catch (IOException | SynapseException ex) {
            System.out.println("[re-upload-tsv] file " + file.getAbsolutePath() + " " + tableId);
            throw new BridgeExporterException("Error uploading TSV as a file handle: " + ex.getMessage(), ex);
        }
        String fileHandleId = tableFileHandle.getId();

        return uploadFileHandleToTable(tableId, fileHandleId);
    }

    public long uploadFileHandleToTable(String tableId, String fileHandleId) throws BridgeExporterException {
        // start tsv import
        CsvTableDescriptor tableDesc = new CsvTableDescriptor();
        tableDesc.setIsFirstLineHeader(true);
        tableDesc.setSeparator("\t");

        String jobToken;
        try {
            jobToken = uploadTsvStartWithRetry(tableId, fileHandleId, tableDesc);
        } catch (SynapseException ex) {
            System.out.println("[re-upload-tsv] filehandle " + fileHandleId + " " + tableId);
            throw new BridgeExporterException("Error starting async import of file handle " + fileHandleId + ": "
                    + ex.getMessage(), ex);
        }

        // poll asyncGet until success or timeout
        boolean success = false;
        Long linesProcessed = null;
        for (int sec = 0; sec < ASYNC_UPLOAD_TIMEOUT_SECONDS; sec++) {
            // sleep for 1 sec
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                // noop
            }

            // poll
            try {
                UploadToTableResult uploadResult = getUploadTsvStatus(jobToken, tableId);
                if (uploadResult == null) {
                    // Result not ready. Sleep some more.
                    continue;
                }

                linesProcessed = uploadResult.getRowsProcessed();
                success = true;
                break;
            } catch (SynapseResultNotReadyException ex) {
                // results not ready, sleep some more
            } catch (SynapseException ex) {
                System.out.println("[re-upload-tsv] filehandle " + fileHandleId + " " + tableId);
                throw new BridgeExporterException("Error polling job status of importing file handle " + fileHandleId
                        + ": " + ex.getMessage(), ex);
            }
        }

        if (!success) {
            throw new BridgeExporterException("Timed out uploading file handle " + fileHandleId);
        }
        if (linesProcessed == null) {
            throw new BridgeExporterException("Null getRowsProcessed()");
        }

        return linesProcessed;
    }

    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS,
            types = { InterruptedException.class, SynapseException.class }, randomize = false)
    public void appendRowsToTableWithRetry(AppendableRowSet rowSet, String tableId) throws InterruptedException,
            SynapseException {
        synapseClient.appendRowsToTable(rowSet, APPEND_TIMEOUT_MILLISECONDS, tableId);
    }

    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public AccessControlList createAclWithRetry(AccessControlList acl) throws SynapseException {
        return synapseClient.createACL(acl);
    }

    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public List<ColumnModel> createColumnModelsWithRetry(List<ColumnModel> columnList) throws SynapseException {
        return synapseClient.createColumnModels(columnList);
    }

    @RetryOnFailure(attempts = 5, delay = 1, unit = TimeUnit.SECONDS,
            types = { AmazonClientException.class, SynapseException.class }, randomize = false)
    public FileHandle createFileHandleWithRetry(File file, String contentType, String parentId) throws IOException,
            SynapseException {
        return synapseClient.createFileHandle(file, contentType, parentId);
    }

    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public TableEntity createTableWithRetry(TableEntity table) throws SynapseException {
        return synapseClient.createEntity(table);
    }

    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public void downloadFileHandleWithRetry(String fileHandleId, File toFile) throws SynapseException {
        synapseClient.downloadFromFileHandleTemporaryUrl(fileHandleId, toFile);
    }

    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public List<ColumnModel> getColumnModelsForTableWithRetry(String tableId) throws SynapseException {
        return synapseClient.getColumnModelsForTableEntity(tableId);
    }

    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public TableEntity getTableWithRetry(String tableId) throws SynapseException {
        return synapseClient.getEntity(tableId, TableEntity.class);
    }

    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public String uploadTsvStartWithRetry(String tableId, String fileHandleId, CsvTableDescriptor tableDescriptor)
            throws SynapseException {
        return synapseClient.uploadCsvToTableAsyncStart(tableId, fileHandleId, null, null, tableDescriptor);
    }

    @RetryOnFailure(attempts = 5, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public UploadToTableResult getUploadTsvStatus(String jobToken, String tableId) throws SynapseException {
        try {
            return synapseClient.uploadCsvToTableAsyncGet(jobToken, tableId);
        } catch (SynapseResultNotReadyException ex) {
            // catch this and return null so we don't retry on "not ready"
            return null;
        }
    }
}
