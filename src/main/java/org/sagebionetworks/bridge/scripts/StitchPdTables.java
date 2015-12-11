package org.sagebionetworks.bridge.scripts;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.util.csv.CsvNullReader;

import org.sagebionetworks.bridge.exporter.BridgeExporterConfig;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.util.BridgeExporterUtil;

/**
 * Given a TSV downloaded from the old table, converts the results to the new table. For voice tables, this involves
 * downloading the file handle ID for momentInDayFormat.json and extracting the choiceAnswers field. For all other
 * tables, the values are copied straight across into the new tables (filling in blank values for saveable and answers
 * as needed).
 *
 * Usage: StitchPdTables [input TSV (old table)] [output TSV (new table)] [new table Synapse ID]
 */
public class StitchPdTables {
    private static final String MOMENT_IN_DAY_FORMAT_JSON = "momentInDayFormat.json";
    private static final Joiner TSV_COLUMN_JOINER = Joiner.on('\t').useForNull("");

    // refactor this - global vars are bad
    private static List<String> outputColumnNameList;
    private static PrintWriter outputPrintWriter;
    private static SynapseHelper synapseHelper;

    public static void main(String[] args) throws IOException, SynapseException {
        // args
        if (args.length != 3) {
            throw new IllegalArgumentException(
                    "Usage: StitchPdTables [input TSV (old table)] [output TSV (new table)] [new table Synapse ID]");
        }
        String inputFilename = args[0];
        String outputFilename = args[1];
        String outputTableId = args[2];

        // init config
        File synapseConfigFile = new File(System.getProperty("user.home") + "/bridge-synapse-exporter-config.json");
        BridgeExporterConfig config = BridgeExporterUtil.JSON_MAPPER.readValue(synapseConfigFile,
                BridgeExporterConfig.class);

        // init synapse client
        SynapseClient synapseClient = new SynapseClientImpl();
        synapseClient.setUserName(config.getUsername());
        synapseClient.setApiKey(config.getApiKey());

        // init synapse helper with synapse client
        synapseHelper = new SynapseHelper();
        synapseHelper.setSynapseClient(synapseClient);

        // query Synapse to get column names for output table
        List<ColumnModel> outputColumnList = synapseHelper.getColumnModelsForTableWithRetry(outputTableId);
        List<String> outputColumnNameList = new ArrayList<>();
        for (ColumnModel oneColumn : outputColumnList) {
            outputColumnNameList.add(oneColumn.getName());
        }

        // set up IO
        Writer outputFileWriter = new OutputStreamWriter(new FileOutputStream(outputFilename), Charsets.UTF_8);
        outputPrintWriter = new PrintWriter(outputFileWriter, true);
        Reader inputFileReader = new InputStreamReader(new FileInputStream(inputFilename), Charsets.UTF_8);
        CsvNullReader inputFileCsvReader = new CsvNullReader(inputFileReader, '\t');

        // get input column names
        String[] inputColumnNameArray = inputFileCsvReader.readNext();

        // write header names
        outputPrintWriter.println(TSV_COLUMN_JOINER.join(outputColumnNameList));

        // read each line in the input and process it
        String[] inputColumnValueArray;
        while ((inputColumnValueArray = inputFileCsvReader.readNext()) != null) {
            try {
                // TODO
            } catch (RuntimeException ex) {
                System.out.println("[ERROR] Error processing row: "
                        + TSV_COLUMN_JOINER.join(inputColumnValueArray));
                ex.printStackTrace(System.out);
            }
        }

        // close streams
        Closeables.close(outputPrintWriter, true);
        Closeables.close(inputFileCsvReader, true);
    }

    private static class Task implements Runnable {
        private final Map<String, String> columnMap;

        public Task(Map<String, String> columnMap) {
            this.columnMap = columnMap;
        }

        @Override
        public void run() {
            try {
                String momentInDayFileHandleId = columnMap.get(MOMENT_IN_DAY_FORMAT_JSON);
                if (!Strings.isNullOrEmpty(momentInDayFileHandleId)) {
                    // We have a momentInDayFormat.json. Download it from Synapse.
                    File tmpFile = File.createTempFile("momentInDayFormat", ".json");
                    synapseHelper.downloadFileHandleWithRetry(momentInDayFileHandleId, tmpFile);
                    byte[] momentInDayJsonBytes = Files.toByteArray(tmpFile);
                    tmpFile.delete();
                    // TODO
                }

                // TODO
            } catch (IOException | SynapseException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
