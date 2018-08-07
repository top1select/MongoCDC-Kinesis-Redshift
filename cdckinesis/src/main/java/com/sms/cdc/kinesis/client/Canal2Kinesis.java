package com.sms.cdc.kinesis.client;


import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.*;
import com.sms.bigdata.seagull.model.CanalEvent;
import com.sms.bigdata.seagull.utils.Config;
import com.sms.bigdata.seagull.utils.KafkaUtils;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Canal2Kinesis {
    final static String destination;
    private static final Logger LOG = LoggerFactory.getLogger(Canal2Kinesis.class);
    final static CanalConnector connector;
    //暂定只处理一个db里面的表
    static String database;
    static String regionName;
    static String streamName;
    static String kinesisKey;
    static String kinesisSecret;
    final static BasicAWSCredentials awsCreds;

    static AmazonKinesis amazonKinesis;

}
