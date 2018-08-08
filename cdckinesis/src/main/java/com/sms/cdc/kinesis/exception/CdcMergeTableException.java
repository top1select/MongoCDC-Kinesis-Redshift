package com.sms.cdc.kinesis.exception;

public class CdcMergeTableException extends CdcException {

    public CdcMergeTableException(String message, Throwable cause) {
        super(message, cause);
    }
}
