package com.sms.cdc.kinesis.exception;

public class CdcException extends Exception {
    public CdcException(String message, Throwable cause) {
        super(message, cause);
    }
}
