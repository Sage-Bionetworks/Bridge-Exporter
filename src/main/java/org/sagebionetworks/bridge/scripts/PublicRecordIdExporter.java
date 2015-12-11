package org.sagebionetworks.bridge.scripts;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;

import org.sagebionetworks.bridge.exporter.BridgeExporterConfig;
import org.sagebionetworks.bridge.exporter.UploadSchemaKey;
import org.sagebionetworks.bridge.util.BridgeExporterUtil;

/**
 * <p>
 * This script queries DDB tables HealthDataRecord and ParticipantOptions to get all recordIds that (a) were set to
 * sharing broadly at the time the upload was recorded and (b) the user is currently set to share broadly. This is a
 * stepping stone to creating public data exports.
 * </p>
 * <p>
 * The output is a TSV, where the first column is the recordId, the second column is the studyId, and the third
 * column is schema key (study-schemaId-vSchemaRev).
 * </p>
 */
@SuppressWarnings("unchecked")
public class PublicRecordIdExporter {
    private static final int PROGRESS_REPORT_INTERVAL = 1000;

    public static void main(String[] args) throws IOException {
        // args
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: PublicRecordIdExporter [output filename]");
        }
        String outputFilename = args[0];

        // init
        File synapseConfigFile = new File(System.getProperty("user.home") + "/bridge-synapse-exporter-config.json");
        BridgeExporterConfig config = BridgeExporterUtil.JSON_MAPPER.readValue(synapseConfigFile,
                BridgeExporterConfig.class);

        // Dynamo DB client - move to Spring
        // This gets credentials from the default credential chain. For developer desktops, this is ~/.aws/credentials.
        // For EC2 instances, this happens transparently.
        // See http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html and
        // http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html#set-up-creds for more
        // info.
        DynamoDB ddbClient = new DynamoDB(new AmazonDynamoDBClient());
        Table recordTable = ddbClient.getTable(config.getBridgeDataDdbPrefix() + "HealthDataRecord3");
        Table participantOptionsTable = ddbClient.getTable(config.getBridgeDataDdbPrefix() + "ParticipantOptions");

        // set up output file
        try (Writer outputFileWriter = new OutputStreamWriter(new FileOutputStream(outputFilename), Charsets.UTF_8);
                PrintWriter outputPrintWriter = new PrintWriter(outputFileWriter, true)) {

            // write TSV headers
            outputPrintWriter.println("recordId\tstudyId\tschemaKey");

            // scan DDB - Don't use scan filters, since the iterator will just keep iterating until it finds a row that
            // matches the filters and consume a lot of throughput.
            int ddbDelay = config.getDdbDelay();
            int numRows = 0;
            int numFiltered = 0;
            Stopwatch stopwatch = Stopwatch.createStarted();
            Iterable<Item> recordIter = recordTable.scan();
            for (Item oneRecord : recordIter) {
                // rate limit DDB calls
                if (ddbDelay > 0) {
                    try {
                        Thread.sleep(ddbDelay);
                    } catch (InterruptedException ex) {
                        // noop
                    }
                }

                if ((++numRows) % PROGRESS_REPORT_INTERVAL == 0) {
                    System.out.println("Found " + numRows + " rows in " + stopwatch.elapsed(TimeUnit.SECONDS)
                            + " seconds");
                }

                try {
                    // extract relevant values
                    String recordId = oneRecord.getString("id");
                    String studyId = oneRecord.getString("studyId");
                    String schemaId = oneRecord.getString("schemaId");
                    int schemaRev = oneRecord.getInt("schemaRevision");
                    UploadSchemaKey schemaKey = new UploadSchemaKey(studyId, schemaId, schemaRev);

                    String healthCode = oneRecord.getString("healthCode");
                    String recordSharingScope = oneRecord.getString("userSharingScope");

                    // filter out by sharing scope in the record
                    if (!"ALL_QUALIFIED_RESEARCHERS".equals(recordSharingScope)) {
                        numFiltered++;
                        continue;
                    }

                    // filter out by participant options table
                    Iterable<Item> optionIterable = participantOptionsTable.query("healthDataCode", healthCode);
                    Iterator<Item> optionIterator = optionIterable.iterator();
                    if (!optionIterator.hasNext()) {
                        // No participant options record. Assume that the user is not sharing.
                        numFiltered++;
                        continue;
                    }

                    // assume there's only one entry
                    Item optionItem = optionIterator.next();
                    String optionJsonText = optionItem.getString("data");
                    Map<String, String> optionMap = BridgeExporterUtil.JSON_MAPPER.readValue(optionJsonText,
                            Map.class);
                    String userSharingScope = optionMap.get("SHARING_SCOPE");
                    if (!"ALL_QUALIFIED_RESEARCHERS".equals(userSharingScope)) {
                        numFiltered++;
                        continue;
                    }

                    // print row
                    outputPrintWriter.print(recordId);
                    outputPrintWriter.print('\t');
                    outputPrintWriter.print(studyId);
                    outputPrintWriter.print('\t');
                    outputPrintWriter.print(schemaKey.toString());
                    outputPrintWriter.println();
                } catch (RuntimeException ex) {
                    System.out.println("Exception processing record: " + ex.getMessage());
                    ex.printStackTrace(System.out);
                }
            }

            System.out.println("Total rows: " + numRows);
            System.out.println("Num rows filtered: " + numFiltered);
            System.exit(0);
        }
    }
}
