package com.sms.cdc.kinesis.utils;

import com.sms.bigdata.seagull.ext.SeagullRuntimException;
import com.sms.bigdata.seagull.utils.Config;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ZKFactory {
    private static final Logger logger = LoggerFactory.getLogger(ZKFactory.class);

    private static final String NAMESPACE;
    private static final String ZK_CONN_STR;

    private static CuratorFramework DEFAULT_ZK_INSTANCE;

    static{
        Config config = new Config("seagull.properties");
        NAMESPACE = config.get("zookeeper.namespace");
        ZK_CONN_STR = config.get("zookeeper.connect");
    }

    private ZKFactory() {
        throw new SeagullRuntimException("illegal instance created");
    }

    public static CuratorFramework newInstance(final String connStr, final String namespace) {
        final CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(connStr)
                .namespace(namespace)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .defaultData(null)
                .build();
        client.start();
        logger.info("successfully created Zookeeper client: host={}, namespace={}", connStr, namespace);
        return client;
    }
    public static CuratorFramework newInstance(final String namespace) {
        return newInstance(ZK_CONN_STR, namespace);
    }

    public static CuratorFramework newInstance() {
        return newInstance(NAMESPACE);
    }

    public static synchronized CuratorFramework get() {
        if (DEFAULT_ZK_INSTANCE == null) {
            DEFAULT_ZK_INSTANCE = newInstance();

        }
        return DEFAULT_ZK_INSTANCE;
    }

    public static synchronized void close() {
        if (DEFAULT_ZK_INSTANCE != null) {
            DEFAULT_ZK_INSTANCE.close();
            logger.info("default Zookeeper client closed.");
        }
    }
}
