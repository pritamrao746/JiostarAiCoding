package com.streaming.heartbeat.exception;

/**
 * Exception thrown when an incoming Kafka message fails schema or business validation rules.
 */
public class ValidationException extends Exception {
    public ValidationException(String message) {
        super(message);
    }

    public ValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}

