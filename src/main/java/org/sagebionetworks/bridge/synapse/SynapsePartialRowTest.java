package org.sagebionetworks.bridge.synapse;

import java.io.File;
import java.io.IOException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.table.PartialRow;
import org.sagebionetworks.repo.model.table.PartialRowSet;

import org.sagebionetworks.bridge.exporter.BridgeExporterConfig;
import org.sagebionetworks.bridge.util.BridgeExporterUtil;

public class SynapsePartialRowTest {
    public static void main(String[] args) throws InterruptedException, IOException, SynapseException {
        // init config
        File synapseConfigFile = new File(System.getProperty("user.home") + "/bridge-synapse-exporter-config.json");
        BridgeExporterConfig config = BridgeExporterUtil.JSON_MAPPER.readValue(synapseConfigFile,
                BridgeExporterConfig.class);

        // init synapse client
        SynapseClient synapseClient = new SynapseClientImpl();
        synapseClient.setUserName(config.getUsername());
        synapseClient.setApiKey(config.getApiKey());
        synapseClient.setRepositoryEndpoint("https://repo-staging.prod.sagebase.org/repo/v1");

        // construct partial row update request
        PartialRow partialRow = new PartialRow();
        partialRow.setRowId(0L);
        partialRow.setValues(ImmutableMap.of("foo", "updated value 3"));

        PartialRowSet partialRowSet = new PartialRowSet();
        partialRowSet.setTableId("syn3735779");
        partialRowSet.setRows(ImmutableList.of(partialRow));

        synapseClient.appendRowsToTable(partialRowSet, 30000, "syn3735779");

        System.out.println("Done");
    }
}
