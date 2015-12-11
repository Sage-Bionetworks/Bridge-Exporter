package org.sagebionetworks.bridge.synapse;

import java.io.File;
import java.io.IOException;

import com.google.common.base.Joiner;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.table.Row;

import org.sagebionetworks.bridge.exporter.BridgeExporterConfig;
import org.sagebionetworks.bridge.util.BridgeExporterUtil;

public class SynapseTableIteratorManualTest {
    private static final Joiner COLUMN_JOINER = Joiner.on("\", \"").useForNull("");

    public static void main(String[] args) throws IOException, SynapseException {
        // init config
        File synapseConfigFile = new File(System.getProperty("user.home") + "/bridge-synapse-exporter-config.json");
        BridgeExporterConfig config = BridgeExporterUtil.JSON_MAPPER.readValue(synapseConfigFile,
                BridgeExporterConfig.class);

        // init synapse client
        SynapseClient synapseClient = new SynapseClientImpl();
        synapseClient.setUserName(config.getUsername());
        synapseClient.setApiKey(config.getApiKey());
        synapseClient.setRepositoryEndpoint("https://repo-staging.prod.sagebase.org/repo/v1");

        // init table iterator
        SynapseTableIterator tableIter = new SynapseTableIterator(synapseClient, null, "syn3503030");
        int numRows = 0;
        while (tableIter.hasNext()) {
            Row row = tableIter.next();
            System.out.println("Row ID=" + row.getRowId() + ", values=(\"" + COLUMN_JOINER.join(row.getValues())
                    + "\")");

            if (++numRows >= 200) {
                break;
            }
        }

        System.out.println("Done");
    }
}
