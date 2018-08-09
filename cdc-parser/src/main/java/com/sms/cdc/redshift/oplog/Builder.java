package com.sms.cdc.redshift.oplog;

import com.sms.bigdata.seagull.model.OP;
import com.sms.bigdata.seagull.model.Oplog;
import com.sms.cdc.redshift.oplog.dml.DeleteParser;
import com.sms.cdc.redshift.oplog.dml.InsertParser;
import com.sms.cdc.redshift.oplog.dml.UpdateParser;

import static com.sms.cdc.redshift.Parser.LOG;

public class Builder {

    public static Builder get() {
        return new Builder();
    }

    public OplogParser build(String json) {
        return build(Oplog.valueOf(json));
    }

    public OplogParser build(Oplog oplog) {
        OP op = oplog.getOp();
        switch (op) {
            case insert:
                return new InsertParser(oplog);
            case delete:
                return new DeleteParser(oplog);
            case update:
                return new UpdateParser(oplog);
            default:
                LOG.error("Unsupported Oplog type: {}", op);
                return null;
        }
    }

}
