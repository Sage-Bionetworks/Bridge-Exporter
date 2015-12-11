package org.sagebionetworks.bridge.exceptions;

@SuppressWarnings("serial")
public class BridgeExporterException extends Exception {
    public BridgeExporterException() {
    }

    public BridgeExporterException(String message) {
        super(message);
    }

    public BridgeExporterException(String message, Throwable cause) {
        super(message, cause);
    }

    public BridgeExporterException(Throwable cause) {
        super(cause);
    }
}
