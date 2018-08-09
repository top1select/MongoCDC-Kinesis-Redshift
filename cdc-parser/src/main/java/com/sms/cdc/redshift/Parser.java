package com.sms.cdc.redshift;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

import java.lang.invoke.MethodHandles;


public interface Parser<R> {
    Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    Parser<R> withTableName(String name);

    R toRedshift();

    R toRedshift(boolean upsert);

}
