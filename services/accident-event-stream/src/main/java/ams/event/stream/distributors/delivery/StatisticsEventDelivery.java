package ams.event.stream.distributors.delivery;

import ams.data.model.AccidentEventModel;
import ams.data.model.AccidentType;
import ams.data.model.StatisticalModel;
import ams.event.stream.serde.AvroSerde;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

@Component
public class StatisticsEventDelivery implements EventDelivery<String, AccidentEventModel>{
    @Value("${topic.config.statistics}")
    private String statisticsTopic;
    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Override
    public void deliverEvent(KStream<String, AccidentEventModel> kStream) {
        kStream.groupBy((key, accident) -> accident.getType().toString(), Grouped.with(
                        AvroSerde.String(schemaRegistryUrl),
                        AvroSerde.AccidentEventModel(schemaRegistryUrl)
                ))
                .windowedBy(
                        TimeWindows.ofSizeWithNoGrace(Duration.of(5, ChronoUnit.MINUTES))
                )
                .count(
                        Named.as("count-by-window-interval"),
                        Materialized.as("count-by-window-interval")
                )
                .toStream()
                .mapValues((windowed, count) -> StatisticalModel.newBuilder()
                        .setCacheId(UUID.randomUUID().toString())
                        .setId(0)
                        .setCount(count)
                        .setType(AccidentType.valueOf(windowed.key()))
                        .setFrom(LocalDateTime.ofInstant(windowed.window().startTime(), ZoneId.systemDefault()))
                        .setEnd(LocalDateTime.ofInstant(windowed.window().endTime(), ZoneId.systemDefault()))
                        .build()
                )
                //.peek((windowed, model) -> System.out.println("["+windowed+"]: "+model))
                .selectKey((windowed, statisticalModel) ->
                                windowed.key(), Named.as("stats-window-key")
                )
                .to(statisticsTopic, Produced.with(
                        AvroSerde.String(schemaRegistryUrl),
                        AvroSerde.StatisticalModel(schemaRegistryUrl)
                ));
    }
}
