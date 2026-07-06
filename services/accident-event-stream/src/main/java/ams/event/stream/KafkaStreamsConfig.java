package ams.event.stream;

import ams.data.model.AccidentEventModel;
import ams.event.stream.handler.AccidentDeserializationErrorRecoverer;
import ams.event.stream.handler.AccidentStreamsUncaughtExceptionHandler;
import ams.event.stream.processors.AccidentKStreamProcessor;
import ams.event.stream.serde.AvroSerde;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.DefaultSslBundleRegistry;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;

import java.util.Map;

@EnableKafka
@EnableKafkaStreams
@Configuration
@RequiredArgsConstructor
public class KafkaStreamsConfig {
    private final AccidentKStreamProcessor accidentKStreamProcessor;
    private final KafkaProperties kafkaProperties;

    //source topic
    @Value("${topic.config.source}")
    private String accidentEventsTopic;
    //dead letter topic
    @Value("${topic.config.source.dlt}")
    private String accidentEventsTopicDLT;
    //one of the sink topics
    @Value("${topic.config.emergency}")
    private String emergencyEventsTopic;
    //one of the sink topics
    @Value("${topic.config.fire-rescue}")
    private String fireRescueEventsTopic;
    //one of the sink topics
    @Value("${topic.config.law-enforcement}")
    private String policeEventsTopic;
    //one of the sink topics
    @Value("${topic.config.statistics}")
    private String statisticsEventsTopic;
    //geo-fenced sensitive-zone incidents
    @Value("${topic.config.sensitive}")
    private String sensitiveEventsTopic;
    //rapid-repeat (possible fraud) alerts
    @Value("${topic.config.fraud}")
    private String fraudEventsTopic;

    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean
    public KStream<String, AccidentEventModel> kStream(StreamsBuilder builder) {
        KStream<String, AccidentEventModel> stream = builder.stream(
                accidentEventsTopic,
                Consumed.with(
                        AvroSerde.String(schemaRegistryUrl),
                        AvroSerde.AccidentEventModel(schemaRegistryUrl)
                )
        );
        //Process KStream
        accidentKStreamProcessor.process(stream);
        return stream;
    }

    /**
     * Without this, any uncaught processing exception uses the default SHUTDOWN_CLIENT response:
     * the topology dies silently while the JVM (and its HTTP health endpoint) stay up — the
     * pipeline is dead but nothing restarts it. REPLACE_THREAD self-heals transient failures;
     * a rapid failure burst (5 within 1s) still shuts the application down so the orchestrator
     * can restart it (made visible by {@code KafkaStreamsHealthIndicator}).
     */
    @Bean
    public StreamsBuilderFactoryBeanConfigurer configurer() {
        return factoryBean -> factoryBean.setStreamsUncaughtExceptionHandler(
                new AccidentStreamsUncaughtExceptionHandler(5, 1000)
        );
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        //TODO: observation is needed to check if this is the right way to do it.
        SslBundles sslBundles = new DefaultSslBundleRegistry();
        Map<String, Object> props = kafkaProperties.buildStreamsProperties(sslBundles);

        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                RecoveringDeserializationExceptionHandler.class);
        props.put(RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER, recoverer());

        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    ConsumerRecordRecoverer recoverer(){
        return new AccidentDeserializationErrorRecoverer(dltByteTemplate(), accidentEventsTopicDLT);
    }

    /**
     * Byte-for-byte producer for the dead-letter topic: undeserializable records must keep
     * their original payload so they can be inspected and replayed exactly.
     */
    @Bean
    public KafkaTemplate<byte[], byte[]> dltByteTemplate(){
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                props, new ByteArraySerializer(), new ByteArraySerializer()));
    }

    @Bean
    public NewTopic accidentEvents(){
        return TopicBuilder
                .name(accidentEventsTopic)
                .replicas(3)
                .partitions(3)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .build();
    }

    @Bean
    public NewTopic emergencyEvents(){
        return TopicBuilder
                .name(emergencyEventsTopic)
                .replicas(3)
                .partitions(3)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .build();
    }

    @Bean
    public NewTopic policeEvents(){
        return TopicBuilder
                .name(policeEventsTopic)
                .replicas(3)
                .partitions(3)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .build();
    }

    @Bean
    public NewTopic fireRescueEvents(){
        return TopicBuilder
                .name(fireRescueEventsTopic)
                .replicas(3)
                .partitions(3)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .build();
    }

    @Bean
    public NewTopic statisticsEvents(){
        return TopicBuilder
                .name(statisticsEventsTopic)
                .replicas(3)
                .partitions(3)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .build();
    }

    @Bean
    public NewTopic sensitiveEvents(){
        return TopicBuilder
                .name(sensitiveEventsTopic)
                .replicas(3)
                .partitions(3)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .build();
    }

    @Bean
    public NewTopic fraudEvents(){
        return TopicBuilder
                .name(fraudEventsTopic)
                .replicas(3)
                .partitions(3)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .build();
    }

    /**
     * The dead-letter topic must be at least as durable as the business topics — it holds the
     * records that already failed once. RF 3 / min ISR 2 matches the main topics. (Existing
     * topics are left untouched; the k8s dev overlay pre-creates it at RF 1.)
     */
    @Bean
    public NewTopic accidentEventsTopicDlt(){
        return TopicBuilder
                .name(accidentEventsTopicDLT)
                .replicas(3)
                .partitions(1)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .build();
    }
}
