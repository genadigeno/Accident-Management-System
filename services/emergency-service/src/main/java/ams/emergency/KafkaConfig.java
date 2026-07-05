package ams.emergency;

import ams.emergency.exception.InvalidEventException;
import ams.emergency.listeners.EmergencyConsumerInterceptor;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.Map;

@Slf4j
@EnableKafka
@Configuration
@RequiredArgsConstructor
public class KafkaConfig {
    private final KafkaProperties kafkaProperties;

    @Value("${spring.application.name}")
    private String groupId;

    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${spring.kafka.consumer.properties.specific.avro.reader}")
    private String specificAvroReader;

    @Value("${kafka.main.topic-dlt}")
    private String deadLetterTopic;

    @Bean
    public ConsumerFactory<Object, Object> consumerFactory() {
        Map<String, Object> consumerProperties = kafkaProperties.buildConsumerProperties();
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //for logging purposes
        consumerProperties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, EmergencyConsumerInterceptor.class.getName());
        consumerProperties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        consumerProperties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, specificAvroReader);
        // Both deserializers are wrapped: an unwrapped KEY deserializer throws inside poll(),
        // where the error handler cannot seek past it — one poison key would stall the
        // partition forever.
        return new DefaultKafkaConsumerFactory<>(
                consumerProperties,
                new ErrorHandlingDeserializer<>(new KafkaAvroDeserializer()),
                new ErrorHandlingDeserializer<>(new KafkaAvroDeserializer())
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory,
            DefaultErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    /**
     * Failure policy:
     * <ul>
     *   <li><b>Poison records</b> (per-record failures raised as
     *       {@link BatchListenerFailedException}) are retried briefly, then published
     *       individually to the dead-letter topic — the rest of the batch continues.</li>
     *   <li><b>Infrastructure failures</b> (database down, connection lost, timeouts) are not
     *       poison: the batch is retried indefinitely with a capped backoff instead of dumping
     *       valid events to the DLT. Idempotent persistence makes these retries safe.</li>
     * </ul>
     * The dead-letter publisher preserves the original key/value and adds DLT_* origin and
     * exception headers so records can be inspected and replayed.
     */
    @Bean
    public DefaultErrorHandler errorHandler() {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate(),
                (record, ex) -> new TopicPartition(deadLetterTopic, -1));

        // Default: infrastructure failures — retry until healed, backing off up to 30s.
        ExponentialBackOff infrastructureBackOff = new ExponentialBackOff(1_000L, 2.0);
        infrastructureBackOff.setMaxInterval(30_000L);

        // Poison records — a few quick retries, then the record goes to the DLT.
        ExponentialBackOffWithMaxRetries poisonBackOff = new ExponentialBackOffWithMaxRetries(3);
        poisonBackOff.setInitialInterval(500L);
        poisonBackOff.setMultiplier(2.0);
        poisonBackOff.setMaxInterval(10_000L);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, infrastructureBackOff);
        errorHandler.setBackOffFunction((record, ex) ->
                isInfrastructureFailure(ex) ? null : poisonBackOff);   // null = default back-off
        errorHandler.addNotRetryableExceptions(
                NullPointerException.class,
                InvalidEventException.class);
        return errorHandler;
    }

    /** Transient infra problems that retrying can heal — never worth dead-lettering events for. */
    private static boolean isInfrastructureFailure(Exception exception) {
        Throwable cause = exception;
        while (cause != null) {
            if (cause instanceof TransientDataAccessException
                    || cause instanceof RecoverableDataAccessException
                    || cause instanceof DataAccessResourceFailureException
                    || cause instanceof CannotCreateTransactionException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    @Bean
    public KafkaTemplate<Object, Object> kafkaTemplate() {
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }
}
