package ams.event.stream.distributors.delivery;

import org.apache.kafka.streams.kstream.KStream;

public interface EventDelivery<K, V> {
    void deliverEvent(KStream<K, V> kStream);
}
