package org.sagebionetworks.bridge.exporter;

import java.util.Iterator;

import org.sagebionetworks.bridge.exceptions.BridgeExporterException;

public abstract class RecordIdSource implements Iterable<String>, Iterator<String> {
    // This class implements iterable out of convenience. It itself is the iterator, so iterator() returns this.
    @Override
    public Iterator<String> iterator() {
        return this;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    /** Initializes the record ID source. Examples include querying DynamoDB or reading a file. */
    public abstract void init() throws BridgeExporterException;
}
