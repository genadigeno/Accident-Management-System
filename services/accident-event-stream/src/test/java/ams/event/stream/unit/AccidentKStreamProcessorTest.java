package ams.event.stream.unit;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AccidentKStreamProcessorTest {
    private static TopologyTestDriver testDriver;
    private static TestInputTopic<String, String> inputTopic;
    private static TestOutputTopic<String, String> outputTopic;

    private static final String INPUT_TOPIC_NAME = "test-input";
    private static final String OUTPUT_TOPIC_NAME = "test-output";

    @BeforeAll
    static void setup(){
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC_NAME, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues((s, s2) -> s2.toUpperCase())
                        .to(OUTPUT_TOPIC_NAME, Produced.with(Serdes.String(), Serdes.String()));

        testDriver = new TopologyTestDriver(builder.build());
        inputTopic = testDriver.createInputTopic(
                INPUT_TOPIC_NAME,
                Serdes.String().serializer(),
                Serdes.String().serializer()
        );
        outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC_NAME,
                Serdes.String().deserializer(),
                Serdes.String().deserializer());
    }

    @Test
    void tesT(){
        inputTopic.pipeInput("hello", "kafka");
        KeyValue<String, String> keyValue = outputTopic.readKeyValue();
        assertEquals("hello", keyValue.key);
        assertEquals("KAFKA", keyValue.value);
    }
}