package com.sms.cdc.redshift.oplog.dml;


import com.sms.bigdata.seagull.model.Oplog;
import com.sms.cdc.redshift.oplog.OplogParser;

public class InsertParser extends OplogParser {

    public InsertParser(Oplog oplog) {
        super(oplog);
    }

    @Override
    public String toRedshift() {
        return insertOrUpdateToJson();
    }

    @Override
    public String toRedshift(boolean upsert) {
        return toRedshift();
    }

}
