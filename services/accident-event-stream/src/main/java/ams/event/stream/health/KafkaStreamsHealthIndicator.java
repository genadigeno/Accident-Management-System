package ams.event.stream.health;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

/**
 * Reports the actual Kafka Streams topology state as an actuator health component
 * ({@code kafkaStreams}). Without it, the topology can die (default uncaught-exception
 * behaviour, or a shutdown from the failure circuit-breaker) while the HTTP server keeps
 * answering {@code UP} — so nothing notices and nothing restarts the router.
 *
 * <p>The component is included in the {@code liveness} health group (see
 * {@code application.properties}), so a Kubernetes liveness probe restarts the pod when the
 * topology is dead.
 */
@Component("kafkaStreams")
@RequiredArgsConstructor
public class KafkaStreamsHealthIndicator implements HealthIndicator {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Override
    public Health health() {
        KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
        if (kafkaStreams == null) {
            return Health.down().withDetail("state", "NOT_CREATED").build();
        }
        KafkaStreams.State state = kafkaStreams.state();
        // CREATED/REBALANCING are transient, healthy startup states; everything on the way
        // down (PENDING_SHUTDOWN, NOT_RUNNING, PENDING_ERROR, ERROR) means the pipeline is dead.
        boolean alive = state == KafkaStreams.State.RUNNING
                || state == KafkaStreams.State.REBALANCING
                || state == KafkaStreams.State.CREATED;
        return (alive ? Health.up() : Health.down())
                .withDetail("state", state.name())
                .build();
    }
}
