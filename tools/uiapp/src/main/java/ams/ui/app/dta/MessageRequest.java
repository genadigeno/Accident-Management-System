package ams.ui.app.dta;

import lombok.Data;

/**
 * A request to generate accident events. The {@code mode} decides the pace:
 * <ul>
 *   <li>{@code AT_ONCE} — send all {@code total} events immediately (a burst);</li>
 *   <li>{@code FIXED_RATE} — send {@code ratePerSecond} events every second until {@code total};</li>
 *   <li>{@code RANGE_RATE} — send a random {@code rateMin}..{@code rateMax}, multiplied by
 *       {@code scale}, every second until {@code total} (the stream-bombarder model: a random
 *       burst scaled by a multiplier).</li>
 * </ul>
 */
@Data
public class MessageRequest {
    private int total;
    private String mode = "AT_ONCE";
    private int ratePerSecond;
    private int rateMin;
    private int rateMax;
    /** Multiplier applied to the RANGE_RATE per-second burst (stream-bombarder's {@code scale}). */
    private int scale = 1;
}
