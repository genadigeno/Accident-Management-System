package ams.event.stream.handler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;

@Slf4j
public class AccidentProductionExceptionHandler extends DefaultProductionExceptionHandler {
    private int errorCounter;

    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record, Exception e) {
        log.error("[Production ERROR]: skipped record : {}, error message : {}", record, e.getMessage());
        errorCounter++;
        //if Production exceptions exceeds 30, stream-app shut down
        if (errorCounter > 30) {
            errorCounter = 0;
            return ProductionExceptionHandlerResponse.FAIL;
        }
        return ProductionExceptionHandlerResponse.CONTINUE;
    }
}
