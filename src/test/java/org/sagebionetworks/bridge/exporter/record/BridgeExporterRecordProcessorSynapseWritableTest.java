package org.sagebionetworks.bridge.exporter.record;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.joda.time.DateTime;
import org.sagebionetworks.client.exceptions.SynapseClientException;
import org.sagebionetworks.schema.adapter.JSONObjectAdapterException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exporter.exceptions.SynapseUnavailableException;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.synapse.SynapseHelper;

@SuppressWarnings("unchecked")
public class BridgeExporterRecordProcessorSynapseWritableTest {

    private static final DateTime DUMMY_REQUEST_DATE_TIME = DateTime.parse("2015-11-04T23:59:59Z");
    private static final BridgeExporterRequest REQUEST = new BridgeExporterRequest.Builder()
            .withEndDateTime(DUMMY_REQUEST_DATE_TIME).withExportType(ExportType.DAILY).withTag("unit-test-tag").build();

    @Test(expectedExceptions = SynapseUnavailableException.class, expectedExceptionsMessageRegExp =
            "Synapse not in writable state")
    public void synapseNotWritable() throws Exception {
        // mock Synapse helper
        SynapseHelper mockSynapseHelper = mock(SynapseHelper.class);
        when(mockSynapseHelper.isSynapseWritable()).thenReturn(false);

        // set up record processor
        BridgeExporterRecordProcessor recordProcessor = new BridgeExporterRecordProcessor();
        recordProcessor.setSynapseHelper(mockSynapseHelper);

        // execute (should throw)
        recordProcessor.processRecordsForRequest(REQUEST);
    }

    @DataProvider(name = "synapseWritableExceptionProvider")
    public Object[][] synapseWritableExceptionProvider() {
        return new Object[][] {
                { JSONObjectAdapterException.class },
                { SynapseClientException.class },
        };
    }

    @Test(dataProvider = "synapseWritableExceptionProvider", expectedExceptions = SynapseUnavailableException.class,
            expectedExceptionsMessageRegExp = "Error calling Synapse.*")
    public void synapseWritableException(Class<? extends Throwable> exceptionClass) throws Exception {
        // mock Synapse helper
        SynapseHelper mockSynapseHelper = mock(SynapseHelper.class);
        when(mockSynapseHelper.isSynapseWritable()).thenThrow(exceptionClass);

        // set up record processor
        BridgeExporterRecordProcessor recordProcessor = new BridgeExporterRecordProcessor();
        recordProcessor.setSynapseHelper(mockSynapseHelper);

        // execute (should throw)
        recordProcessor.processRecordsForRequest(REQUEST);
    }
}
