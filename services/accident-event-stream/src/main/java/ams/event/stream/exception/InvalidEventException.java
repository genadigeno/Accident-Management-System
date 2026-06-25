package ams.event.stream.exception;

public class InvalidEventException extends RuntimeException {
    public InvalidEventException() {
        super("invalid event received");
    }
}
