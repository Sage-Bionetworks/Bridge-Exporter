package org.sagebionetworks.bridge.scripts;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.io.Files;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.table.Row;
import org.sagebionetworks.repo.model.table.RowSelection;

import org.sagebionetworks.bridge.exporter.BridgeExporterConfig;
import org.sagebionetworks.bridge.synapse.SynapseTableIterator;
import org.sagebionetworks.bridge.util.BridgeExporterUtil;

/**
 * This script takes in a file with a list of health codes and a Synapse table ID. It queries the Synapse table and
 * verifies that all affected rows are on version 1.0, then deletes these rows.
 */
public class InternationalFiltering {
    private static final Joiner HEALTH_CODE_QUERY_JOINER = Joiner.on("', '");
    private static final int NUM_HEALTH_CODES_PER_PAGE = 4500;

    public static void main(String[] args) throws IOException, SynapseException {
        // args
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "Usage: InternationalFiltering [input filename] [target Synapse table ID");
        }
        String inputFilename = args[0];
        String synapseTableId = args[1];

        // Load health codes from file. It's on the order of 5k healthcodes, so we can load it all into memory.
        List<String> healthCodeList = Files.readLines(new File(inputFilename), Charsets.UTF_8);

        // init config
        File synapseConfigFile = new File(System.getProperty("user.home") + "/bridge-synapse-exporter-config.json");
        BridgeExporterConfig config = BridgeExporterUtil.JSON_MAPPER.readValue(synapseConfigFile,
                BridgeExporterConfig.class);

        // init synapse client
        SynapseClient synapseClient = new SynapseClientImpl();
        synapseClient.setUserName(config.getUsername());
        synapseClient.setApiKey(config.getApiKey());

        // We have to paginate health codes, since max query size is 262144 bytes.
        int numHealthCodes = healthCodeList.size();
        List<Long> rowIdsToDelete = new ArrayList<>();
        String etag = null;
        for (int i = 0; i < numHealthCodes; i += NUM_HEALTH_CODES_PER_PAGE) {
            int endIdx = Math.min(i + NUM_HEALTH_CODES_PER_PAGE, numHealthCodes);

            // Make query against the Synapse table for row IDs to delete. This query should select the appVersion so
            // we can do final validation to make sure it's all v1, as well as health code for debugging. This query
            // should also not select any additional columns for performance.
            String sql = "SELECT appVersion, healthCode FROM " + synapseTableId + " WHERE healthCode IN ('"
                    + HEALTH_CODE_QUERY_JOINER.join(healthCodeList.subList(i, endIdx)) + "')";
            SynapseTableIterator tableIter = new SynapseTableIterator(synapseClient, sql, synapseTableId);
            etag = tableIter.getEtag();

            while (tableIter.hasNext()) {
                Row oneRow = tableIter.next();

                // First column is appVersion. Second column is healthCode. Again, null appVersion counts as version
                // 1.0.
                List<String> rowValueList = oneRow.getValues();
                String appVersion = rowValueList.get(0);
                String healthCode = rowValueList.get(1);
                if (!Strings.isNullOrEmpty(appVersion) && !appVersion.startsWith(BridgeExporterUtil.V1_PREFIX)) {
                    throw new IllegalStateException("Found non-V1 app version " + appVersion + " for health code "
                            + healthCode);
                }

                rowIdsToDelete.add(oneRow.getRowId());
            }
        }

        // short-circuit if there are no dupes
        if (rowIdsToDelete.isEmpty()) {
            System.out.println("No rows to delete");
            return;
        }

        // delete rows from Synapse table
        //RowSelection rowSelectionToDelete = new RowSelection();
        //rowSelectionToDelete.setEtag(etag);
        //rowSelectionToDelete.setRowIds(rowIdsToDelete);
        //rowSelectionToDelete.setTableId(synapseTableId);
        //synapseClient.deleteRowsFromTable(rowSelectionToDelete);
        //
        //System.out.println("Deleted " + rowIdsToDelete.size() + " rows");
        System.out.println("Would delete " + rowIdsToDelete.size() + " rows");
    }
}
