package org.sagebionetworks.bridge.exporter.request;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import org.mockito.ArgumentCaptor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exporter.record.BridgeExporterRecordProcessor;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;

public class BridgeExporterSqsCallbackTest {
    @Test
    public void test() throws Exception {
        // Basic test that tests data flow. JSON parsing is already tested by BridgeExporterRequestTest.

        // set up test callback
        BridgeExporterRecordProcessor mockRecordProcessor = mock(BridgeExporterRecordProcessor.class);

        BridgeExporterSqsCallback callback = new BridgeExporterSqsCallback();
        callback.setRecordProcessor(mockRecordProcessor);

        // execute and verify
        callback.callback("{\"endDateTime\":\"2015-10-31T23:59:59.000Z\", \"exportType\":\"DAILY\"}");

        ArgumentCaptor<BridgeExporterRequest> requestCaptor = ArgumentCaptor.forClass(BridgeExporterRequest.class);
        verify(mockRecordProcessor).processRecordsForRequest(requestCaptor.capture());
        BridgeExporterRequest request = requestCaptor.getValue();
        assertEquals(request.getEndDateTime().toString(), "2015-10-31T23:59:59.000Z");
    }

    @DataProvider(name = "malformedRequestProvider")
    public Object[][] malformedRequestProvider() {
        // null is not tested, as there are no observed cases of an SQS message containing a null message.
        return new Object[][] {
                { "" },
                { "   " },
                { "This is not JSON" },
                {
                        "{\n"
                        + "     \"endDateTime\":\"2016-08-13T0:00-0700\",\n"
                        + "     \"tag\":\"invalid request\"\n"
                        + "}"
                },
        };
    }

    @Test(dataProvider = "malformedRequestProvider", expectedExceptions = PollSqsWorkerBadRequestException.class)
    public void malformedRequest(String messageBody) throws Exception {
        BridgeExporterSqsCallback callback = new BridgeExporterSqsCallback();
        callback.callback(messageBody);
    }
}
