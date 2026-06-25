package ams.event.stream;

import ams.data.model.AccidentEventModel;
import ams.data.model.DeserializationErrorResponse;
import ams.event.stream.handler.AccidentDeserializationErrorRecoverer;
import ams.event.stream.processors.AccidentKStreamProcessor;
import ams.event.stream.serde.AvroSerde;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
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

    /*@Bean
    public StreamsBuilderFactoryBeanConfigurer configurer() {
        return factoryBean -> factoryBean.setStreamsUncaughtExceptionHandler(
                new AccidentStreamsUncaughtExceptionHandler(5, 1000)
        );
    }*/

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
        return new AccidentDeserializationErrorRecoverer(kafkaTemplate(), accidentEventsTopicDLT);
    }

    @Bean
    public KafkaTemplate<String, DeserializationErrorResponse> kafkaTemplate(){
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
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

    @Bean
    public NewTopic accidentEventsTopicDlt(){
        return TopicBuilder
                .name(accidentEventsTopicDLT)
                .replicas(1)
                .partitions(1)
                .build();
    }
    @Bean
    public NewTopic accidentEventsTopicCopied(){
        return TopicBuilder
                .name(accidentEventsTopic+".copied")
                .replicas(3)
                .partitions(3)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .build();
    }
}
