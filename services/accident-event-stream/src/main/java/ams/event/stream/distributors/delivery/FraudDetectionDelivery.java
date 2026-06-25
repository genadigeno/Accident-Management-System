package ams.event.stream.distributors.delivery;

import ams.data.model.AccidentEventModel;
import ams.event.stream.serde.AvroSerde;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Rapid-repeat ("possible fraud") detection. Counts incidents per location in a tumbling time
 * window; when a single location is reported more than the configured threshold within the
 * window, a fraud alert is emitted to the fraud topic and a {@code ams.fraud.flagged} metric is
 * raised. Done entirely in-stream (no external rate-limiter needed).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FraudDetectionDelivery implements EventDelivery<String, AccidentEventModel> {

    @Value("${topic.config.fraud}")
    private String fraudTopic;
    @Value("${fraud.window-minutes:5}")
    private long windowMinutes;
    @Value("${fraud.threshold:5}")
    private long threshold;
    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    private final MeterRegistry meterRegistry;

    @Override
    public void deliverEvent(KStream<String, AccidentEventModel> kStream) {
        kStream.groupBy((key, accident) -> location(accident), Grouped.with(
                        AvroSerde.String(schemaRegistryUrl),
                        AvroSerde.AccidentEventModel(schemaRegistryUrl)
                ))
                .windowedBy(
                        TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(windowMinutes))
                )
                .count(
                        Named.as("count-by-location"),
                        Materialized.as("count-by-location")
                )
                .toStream()
                .filter((windowed, count) -> count != null && count > threshold)
                .peek((windowed, count) -> {
                    meterRegistry.counter("ams.fraud.flagged").increment();
                    log.warn("Possible fraud: location '{}' reported {} times in window [{} - {}]",
                            windowed.key(), count, windowed.window().startTime(), windowed.window().endTime());
                })
                .map((windowed, count) -> new KeyValue<>(
                        windowed.key(),
                        String.format("POSSIBLE FRAUD: location '%s' reported %d times in window [%s - %s]",
                                windowed.key(), count, windowed.window().startTime(), windowed.window().endTime())
                ))
                .to(fraudTopic, Produced.with(
                        AvroSerde.String(schemaRegistryUrl),
                        AvroSerde.String(schemaRegistryUrl)
                ));
    }

    private static String location(AccidentEventModel accident) {
        if (accident.getLocation() == null || accident.getLocation().getAddress() == null) {
            return "unknown";
        }
        return accident.getLocation().getAddress().toString();
    }
}
