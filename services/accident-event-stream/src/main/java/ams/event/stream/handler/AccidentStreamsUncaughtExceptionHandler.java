package ams.event.stream.handler;

import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class AccidentStreamsUncaughtExceptionHandler implements StreamsUncaughtExceptionHandler {
    final int maxFailures;
    final long timeIntervalThresholdMillis;
    private Instant previousErrorTime;
    private int failureCount;

    public AccidentStreamsUncaughtExceptionHandler(int maxFailures, long timeIntervalThresholdMillis) {
        this.maxFailures = maxFailures;
        this.timeIntervalThresholdMillis = timeIntervalThresholdMillis;
    }

    @Override
    public StreamThreadExceptionResponse handle(Throwable throwable) {
        failureCount++;
        Instant currentErrorTime = Instant.now();

        if (previousErrorTime == null) {
            previousErrorTime = currentErrorTime;
        }

        //time interval between previous and current error
        long millisBetweenFailure = ChronoUnit.MILLIS.between(previousErrorTime, currentErrorTime);

        //if `currentFailureCount` exceeds `maxFailures` threshold(5)
        if (failureCount >= maxFailures) {
            //if in small interval(`maxTimeIntervalMillis`) we get too many errors, our application will be shut down
            if (millisBetweenFailure <= timeIntervalThresholdMillis) {
                return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
            } else {
                //reset if we still get many error but the interval is relatively long
                failureCount = 0;
                previousErrorTime = null;
            }
        }
        return StreamThreadExceptionResponse.REPLACE_THREAD;
    }
}
