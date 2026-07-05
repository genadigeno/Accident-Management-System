package ams.event.stream.distributors.delivery;

import ams.data.model.AccidentEventModel;
import ams.event.stream.serde.AvroSerde;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Set;

/**
 * Rapid-repeat ("possible fraud") detection. Counts incidents per location in a tumbling time
 * window; when a single location is reported more than the configured threshold within the
 * window, a fraud alert is emitted to the fraud topic and a {@code ams.fraud.flagged} metric is
 * raised. Done entirely in-stream (no external rate-limiter needed).
 *
 * <p>Exactly ONE alert is raised per (location, window): further reports in an already-flagged
 * window are suppressed by a changelogged window store, so a hot window does not spam an alert
 * for every additional event.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FraudDetectionDelivery implements EventDelivery<String, AccidentEventModel> {

    private static final String ALERTED_STORE = "fraud-alerted-windows";

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
        Duration windowSize = Duration.ofMinutes(windowMinutes);
        kStream.groupBy((key, accident) -> location(accident), Grouped.with(
                        AvroSerde.String(schemaRegistryUrl), AvroSerde.AccidentEventModel(schemaRegistryUrl)))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(windowSize))
                .count(Named.as("count-by-location"), Materialized.as("count-by-location"))
                .toStream()
                .filter((windowed, count) -> count != null && count > threshold)
                // Alert exactly once per (location, window) — without this, every further report
                // in a hot window re-emits an alert ("6 times", "7 times", "8 times", ...).
                .processValues(new AlertOncePerWindow(windowSize))
                .peek((windowed, count) -> {
                    meterRegistry.counter("ams.fraud.flagged").increment();
                    log.warn("Possible fraud: location '{}' reported {} times in window [{} - {}]",
                            windowed.key(), count, windowed.window().startTime(), windowed.window().endTime());
                })
                .map((windowed, count) -> new KeyValue<>(
                        windowed.key(),
                        String.format("POSSIBLE FRAUD: location '%s' reported %d times in window [%s - %s]",
                                windowed.key(), count, windowed.window().startTime(), windowed.window().endTime())))
                .to(fraudTopic, Produced.with(
                        AvroSerde.String(schemaRegistryUrl),
                        AvroSerde.String(schemaRegistryUrl)));
    }

    private static String location(AccidentEventModel accident) {
        if (accident.getLocation() == null || accident.getLocation().getAddress() == null) {
            return "unknown";
        }
        return accident.getLocation().getAddress().toString();
    }

    /**
     * Forwards only the FIRST above-threshold count for each (location, window); later updates
     * of an already-alerted window are dropped. Backed by a changelogged window store, so the
     * once-only guarantee survives restarts and is transactional under exactly-once processing.
     */
    private static final class AlertOncePerWindow
            implements FixedKeyProcessorSupplier<Windowed<String>, Long, Long> {

        private final Duration windowSize;

        private AlertOncePerWindow(Duration windowSize) {
            this.windowSize = windowSize;
        }

        @Override
        public FixedKeyProcessor<Windowed<String>, Long, Long> get() {
            return new FixedKeyProcessor<>() {
                private FixedKeyProcessorContext<Windowed<String>, Long> context;
                private WindowStore<String, Long> alerted;

                @Override
                public void init(FixedKeyProcessorContext<Windowed<String>, Long> context) {
                    this.context = context;
                    this.alerted = context.getStateStore(ALERTED_STORE);
                }

                @Override
                public void process(FixedKeyRecord<Windowed<String>, Long> record) {
                    String location = record.key().key();
                    long windowStart = record.key().window().start();
                    if (alerted.fetch(location, windowStart) == null) {
                        alerted.put(location, record.value(), windowStart);
                        context.forward(record);
                    }
                }
            };
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            return Set.of(Stores.windowStoreBuilder(
                    Stores.persistentWindowStore(ALERTED_STORE, windowSize.multipliedBy(2), windowSize, false),
                    Serdes.String(), Serdes.Long()));
        }
    }
}
