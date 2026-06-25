package ams.ui.app.dta;

import ams.ui.app.model.SendBatch;

/** Immutable snapshot of a {@link SendBatch}, sent to the dashboard over REST/WebSocket. */
public record SendBatchView(
        String id,
        int total,
        int produced,
        int failed,
        String status,
        long startedAt,
        long finishedAt) {

    public static SendBatchView from(SendBatch b) {
        return new SendBatchView(
                b.getId(),
                b.getTotal(),
                b.getProduced(),
                b.getFailed(),
                b.getStatus().name(),
                b.getStartedAt(),
                b.getFinishedAt());
    }
}
