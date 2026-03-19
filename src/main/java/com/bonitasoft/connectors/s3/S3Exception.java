package com.bonitasoft.connectors.s3;

public class S3Exception extends Exception {

    private final int statusCode;
    private final boolean retryable;

    public S3Exception(String message) {
        super(message);
        this.statusCode = -1;
        this.retryable = false;
    }

    public S3Exception(String message, Throwable cause) {
        super(message, cause);
        this.statusCode = -1;
        this.retryable = false;
    }

    public S3Exception(String message, int statusCode, boolean retryable) {
        super(message);
        this.statusCode = statusCode;
        this.retryable = retryable;
    }

    public S3Exception(String message, int statusCode, boolean retryable, Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
        this.retryable = retryable;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public boolean isRetryable() {
        return retryable;
    }
}
