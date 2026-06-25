package ams.event.stream.serde;

import ams.data.model.*;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.PrimitiveAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;

import java.util.HashMap;
import java.util.Map;

public final class AvroSerde {
    private AvroSerde() {}

    public static PrimitiveAvroSerde<String> String(String url) {
        PrimitiveAvroSerde<String> keySerde = new PrimitiveAvroSerde<>();
        configureSerde(keySerde, true, url);
        return keySerde;
    }

    public static PrimitiveAvroSerde<String> String(boolean isKey, String url) {
        PrimitiveAvroSerde<String> keySerde = new PrimitiveAvroSerde<>();
        configureSerde(keySerde, isKey, url);
        return keySerde;
    }

    public static SpecificAvroSerde<StatisticalModel> StatisticalModel(String url){
        SpecificAvroSerde<StatisticalModel> valueSerde = new SpecificAvroSerde<>();
        configureSerde(valueSerde, false, url);
        return valueSerde;
    }

    public static SpecificAvroSerde<FireAccidentModel> FireAccidentModel(String url){
        SpecificAvroSerde<FireAccidentModel> valueSerde = new SpecificAvroSerde<>();
        configureSerde(valueSerde, false, url);
        return valueSerde;
    }

    public static SpecificAvroSerde<PoliceEventModel> PoliceEventModel(String url){
        SpecificAvroSerde<PoliceEventModel> valueSerde = new SpecificAvroSerde<>();
        configureSerde(valueSerde, false, url);
        return valueSerde;
    }

    public static SpecificAvroSerde<EmergencyEventModel> EmergencyEventModel(String url){
        SpecificAvroSerde<EmergencyEventModel> valueSerde = new SpecificAvroSerde<>();
        configureSerde(valueSerde, false, url);
        return valueSerde;
    }

    public static SpecificAvroSerde<AccidentEventModel> AccidentEventModel(String url) {
        SpecificAvroSerde<AccidentEventModel> valueSerde = new SpecificAvroSerde<>();
        configureSerde(valueSerde, false, url);
        return valueSerde;
    }

    private static void configureSerde(Serde<?> serde, boolean isKey, String url) {
        Map<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, url);
        serdeConfig.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        //serdeConfig.put("specific.avro.reader.value.subject.version", "latest");
        serde.configure(serdeConfig, isKey);
    }
}
