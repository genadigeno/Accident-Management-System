package ams.enrichment;

import ams.data.model.EnrichedAccidentEvent;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.LinkedHashMap;
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

    @Value("${kafka.enriched.topic}")
    private String enrichedTopic;

    @Bean
    public ConsumerFactory<Object, Object> consumerFactory() {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, specificAvroReader);
        // Both deserializers are wrapped: an unwrapped KEY deserializer throws inside poll(),
        // where the error handler cannot seek past it.
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
     * Poison records ({@link BatchListenerFailedException}) retry briefly then dead-letter
     * individually. Enrichment degrades gracefully (a weather-API outage never throws), so the
     * only expected failure is an undeserializable record.
     */
    @Bean
    public DefaultErrorHandler errorHandler() {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate(),
                (record, ex) -> new TopicPartition(deadLetterTopic, -1));

        ExponentialBackOff infrastructureBackOff = new ExponentialBackOff(1_000L, 2.0);
        infrastructureBackOff.setMaxInterval(30_000L);

        ExponentialBackOffWithMaxRetries poisonBackOff = new ExponentialBackOffWithMaxRetries(3);
        poisonBackOff.setInitialInterval(500L);
        poisonBackOff.setMultiplier(2.0);
        poisonBackOff.setMaxInterval(10_000L);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, poisonBackOff);
        errorHandler.addNotRetryableExceptions(NullPointerException.class);
        return errorHandler;
    }

    /** Enriched-event topic, published for downstream consumers (RF 3 / min ISR 2). */
    @Bean
    public NewTopic enrichedTopicDefinition() {
        return TopicBuilder.name(enrichedTopic)
                .partitions(3)
                .replicas(3)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .build();
    }

    /** Dead-letter topic, at least as durable as the source (RF 3 / min ISR 2). */
    @Bean
    public NewTopic deadLetterTopicDefinition() {
        return TopicBuilder.name(deadLetterTopic)
                .partitions(1)
                .replicas(3)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .build();
    }

    /** Avro producer for the enriched events, keyed by cacheId. */
    @Bean
    public KafkaTemplate<String, EnrichedAccidentEvent> enrichedKafkaTemplate() {
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    /**
     * Producer used by the dead-letter recoverer. Serializers are selected by payload type:
     * records that failed DESERIALIZATION are re-published as their original raw bytes, while
     * records that failed in the listener re-serialize through Avro.
     */
    @Bean
    public KafkaTemplate<Object, Object> kafkaTemplate() {
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                props, byteAwareAvroSerializer(), byteAwareAvroSerializer()));
    }

    private static DelegatingByTypeSerializer byteAwareAvroSerializer() {
        Map<Class<?>, Serializer<?>> delegates = new LinkedHashMap<>();
        delegates.put(byte[].class, new ByteArraySerializer());
        delegates.put(Object.class, new KafkaAvroSerializer());
        return new DelegatingByTypeSerializer(delegates, true);
    }
}
