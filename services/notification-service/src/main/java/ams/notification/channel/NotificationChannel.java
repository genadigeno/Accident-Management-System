package ams.notification.channel;

import ams.notification.domain.Notification;

/**
 * A delivery channel for notifications. Implementations must be side-effect-safe to call once
 * per recorded notification; failures are reported via the return value, never thrown.
 *
 * <p>To add SMTP, Telegram, SMS, ... : implement this interface as a {@code @Component} (gate it
 * with {@code @ConditionalOnProperty} on its config) — the service picks up every enabled
 * channel automatically.
 */
public interface NotificationChannel {

    String name();

    /** @return true when delivered, false when the attempt failed (recorded, not thrown). */
    boolean send(Notification notification);
}
