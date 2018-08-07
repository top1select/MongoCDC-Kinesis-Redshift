package com.sms.cdc.kinesis.mongo;

import com.google.common.base.Preconditions;
import com.sms.bigdata.seagull.utils.Config;
import com.sms.bigdata.seagull.utils.KafkaUtils;
import com.sms.cdc.kinesis.impl.OffsetProviderByRedis;
import com.sms.cdc.kinesis.impl.OffsetProviderByZK;
import com.sms.cdc.kinesis.utils.ZKFactory;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BootStrap {
    private static final Logger logger = LoggerFactory.getLogger(BootStrap.class);
    private static final String tailerName = "Seagull";

    public static void main(String[] args) throws InterruptedException {
        Config config = new Config("seagull.properties");
        final String TOPIC = config.get("seagull.mongo.topic");
        final String basePath = config.get("tailer.zk.path");
        Preconditions.checkNotNull(basePath);

        String host = config.get("redis.host");
        int port = NumberUtils.toInt(config.get("redis.port"));
        final OffsetProvider offsetProvider = new OffsetProviderByRedis(host,port, tailerName);
        //deprecate this mongo url tyle
        String mongoUrl = config.get("mongodb.url");

        Tailer tailer = new Tailer(tailerName, mongoUrl, offsetProvider, TOPIC);
        while(true){
            tailer.run();

            Thread.sleep(1000);

        }


    }
}
