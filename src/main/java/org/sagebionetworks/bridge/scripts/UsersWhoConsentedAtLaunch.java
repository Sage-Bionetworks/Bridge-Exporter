package org.sagebionetworks.bridge.scripts;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ScanFilter;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import org.joda.time.DateTime;

import org.sagebionetworks.bridge.exporter.BridgeExporterConfig;
import org.sagebionetworks.bridge.util.BridgeExporterUtil;

/**
 * Helper class to query DDB for users who consented at launch (eg, on the first two days). We do this in a Java app
 * because the DDB web UI limits the results to the first 600, while we want the full list.
 *
 * This writes a list of health codes, one per line, to the specified output file.
 */
public class UsersWhoConsentedAtLaunch {
    private static final long CONSENT_END_TIME_MILLIS = DateTime.parse("2015-03-11T00:00-0700").getMillis();
    private static final int PROGRESS_REPORT_INTERVAL = 1000;

    public static void main(String[] args) throws IOException {
        // args
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: UsersWhoConsentedAtLaunch [output filename]");
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
        Table userConsentTable = ddbClient.getTable("prod-heroku-UserConsent2");

        // set up output file
        Writer outputFileWriter = new OutputStreamWriter(new FileOutputStream(outputFilename), Charsets.UTF_8);
        PrintWriter outputPrintWriter = new PrintWriter(outputFileWriter, true);

        // scan DDB
        int ddbDelay = config.getDdbDelay();
        int numRows = 0;
        Stopwatch stopwatch = Stopwatch.createStarted();
        Iterable<Item> consentIter = userConsentTable.scan(new ScanFilter("signedOn").le(CONSENT_END_TIME_MILLIS));
        for (Item oneConsent : consentIter) {
            // rate limit DDB calls
            if (ddbDelay > 0) {
                try {
                    Thread.sleep(ddbDelay);
                } catch (InterruptedException ex) {
                    // noop
                }
            }

            outputPrintWriter.println(oneConsent.getString("healthCode"));

            if ((++numRows) % PROGRESS_REPORT_INTERVAL == 0) {
                System.out.println("Found " + numRows + " rows in " + stopwatch.elapsed(TimeUnit.SECONDS)
                        + " seconds");
            }
        }

        System.out.println("Total: " + numRows + " rows");
    }
}
