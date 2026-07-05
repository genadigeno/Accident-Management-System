package ams.statistics;

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
        Map<String, Object> props = kafkaProperties.buildConsumerProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, specificAvroReader);
        // Both deserializers are wrapped: an unwrapped KEY deserializer throws inside poll(),
        // where the error handler cannot seek past it — one poison key would stall the
        // partition forever.
        return new DefaultKafkaConsumerFactory<>(
                props,
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
     * On unrecoverable errors, publishes the original record (key, value bytes and headers
     * preserved) to the dead-letter topic, enriched with DLT_* origin and exception headers
     * so it can be inspected and replayed. Retries transient failures with exponential backoff.
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
        errorHandler.addNotRetryableExceptions(NullPointerException.class);
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
