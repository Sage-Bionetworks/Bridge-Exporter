package org.sagebionetworks.bridge.scripts;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import org.sagebionetworks.util.csv.CsvNullReader;

import org.sagebionetworks.bridge.util.BridgeExporterUtil;

/**
 * Gets users who never upgraded, by parsing the results of the following query against the appVersion table:
 * SELECT healthCode, max(appVersion) FROM syn3420228 group by healthCode
 *
 * The input TSV is the query on appVersion table, on healthCode and on max(appVersion) group by appVersion. This
 * shouldn't contain ROW_ID or ROW_VERSION. The first column should be healthCode and the second column should be
 * max(appVersion). This TSV should also not contain headers.
 *
 * Writes results to the specified file as a list of health codes that never upgraded, one health code per line.
 */
public class UsersWhoNeverUpgraded {
    public static void main(String[] args) throws IOException {
        // args
        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: UsersWhoNeverUpgraded [input TSV] [output file]");
        }
        String inputFilename = args[0];
        String outputFilename = args[1];

        // load TSV
        Reader inputFileReader = new InputStreamReader(new FileInputStream(inputFilename), Charsets.UTF_8);
        CsvNullReader inputTsvReader = new CsvNullReader(inputFileReader, '\t');

        // setup output file writer
        Writer outputFileWriter = new OutputStreamWriter(new FileOutputStream(outputFilename), Charsets.UTF_8);
        PrintWriter outputPrintWriter = new PrintWriter(outputFileWriter, true);

        // Iterate over table to find the health codes that never upgraded past version 1.
        String[] columns;
        while ((columns = inputTsvReader.readNext()) != null) {
            String healthCode = columns[0];
            if (Strings.isNullOrEmpty(healthCode)) {
                // We somehow have an empty health code. Skip this.
                continue;
            }

            String maxAppVersion = columns[1];
            if (Strings.isNullOrEmpty(maxAppVersion) || maxAppVersion.startsWith(BridgeExporterUtil.V1_PREFIX)) {
                // null appVersion counts as version 1.0, since some uploads in early versions didn't set the
                // appVersion.
                outputPrintWriter.println(healthCode);
            }
        }
    }
}
