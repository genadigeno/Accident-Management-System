package ams.ui.app.dta;

import lombok.Data;

/**
 * A request to generate accident events. The {@code mode} decides the pace:
 * <ul>
 *   <li>{@code AT_ONCE} — send all {@code total} events immediately (a burst);</li>
 *   <li>{@code FIXED_RATE} — send {@code ratePerSecond} events every second until {@code total};</li>
 *   <li>{@code RANGE_RATE} — send a random {@code rateMin}..{@code rateMax} every second until {@code total}.</li>
 * </ul>
 */
@Data
public class MessageRequest {
    private int total;
    private String mode = "AT_ONCE";
    private int ratePerSecond;
    private int rateMin;
    private int rateMax;
}
