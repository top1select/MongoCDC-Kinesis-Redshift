package com.sms.cdc.kinesis.exception;


public class CdcBatchException extends CdcException {
    public CdcBatchException(String message, Throwable cause) {
        super(message, cause);
    }
}
