package org.sagebionetworks.bridge.retry;

import java.util.concurrent.TimeUnit;

import com.jcabi.aspects.RetryOnFailure;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import org.sagebionetworks.bridge.util.BridgeExporterUtil;

public class RetryHelloWorld {
    public static void main(String[] args) {
        new RetryHelloWorld().call();
    }

    private int numCalled = 0;

    @RetryOnFailure(attempts = 10, delay = 1, unit = TimeUnit.SECONDS, randomize = false)
    public void call() {
        System.out.println("Called " + ++numCalled + " times, " + DateTime.now(BridgeExporterUtil.LOCAL_TIME_ZONE)
                .toString(ISODateTimeFormat.dateTime()));
        throw new RuntimeException("foo");
    }
}
