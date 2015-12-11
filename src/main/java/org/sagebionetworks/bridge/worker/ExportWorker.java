package org.sagebionetworks.bridge.worker;

public class ExportWorker implements Runnable {
    private final ExportHandler handler;
    private final ExportTask task;

    public ExportWorker(ExportHandler handler, ExportTask task) {
        this.handler = handler;
        this.task = task;
    }

    @Override
    public void run() {
        handler.handle(task);
    }
}
