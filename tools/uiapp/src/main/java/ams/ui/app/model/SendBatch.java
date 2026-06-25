package ams.ui.app.model;

import lombok.Getter;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Live tracking state for one "generate events" batch. Updated from the Kafka producer's
 * ack callbacks (one per record), so the counters are thread-safe.
 */
@Getter
public class SendBatch {

    public enum Status {IN_PROGRESS, COMPLETED, COMPLETED_WITH_ERRORS}

    private final String id;
    private final int total;
    private final long startedAt;

    private final AtomicInteger produced = new AtomicInteger();
    private final AtomicInteger failed = new AtomicInteger();
    private final AtomicInteger completed = new AtomicInteger();

    private volatile Status status = Status.IN_PROGRESS;
    private volatile long finishedAt = 0L;

    public SendBatch(String id, int total) {
        this.id = id;
        this.total = total;
        this.startedAt = System.currentTimeMillis();
    }

    public int getProduced() {
        return produced.get();
    }

    public int getFailed() {
        return failed.get();
    }

    /**
     * Records one producer result. Returns {@code true} exactly once — for the result that
     * completes the batch — so the caller can publish a final status update.
     */
    public boolean recordResult(boolean success) {
        if (success) {
            produced.incrementAndGet();
        } else {
            failed.incrementAndGet();
        }
        if (completed.incrementAndGet() == total) {
            finishedAt = System.currentTimeMillis();
            status = failed.get() == 0 ? Status.COMPLETED : Status.COMPLETED_WITH_ERRORS;
            return true;
        }
        return false;
    }
}
