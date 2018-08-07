package com.sms.cdc.kinesis.mongo;


import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.protobuf.util.JsonFormat;
import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import com.sms.bigdata.seagull.model.Oplog;
import com.sms.bigdata.seagull.utils.Config;
//import com.sms.cdc.kinesis.utils.RedshiftUtil;
//import com.sms.cdc.redshift.oplog.Builder;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.Producer;
import org.apache.zookeeper.Op;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.parsing.combinator.testing.Str;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 *
 */
public class Tailer implements Runnable, AutoCloseable, Cloneable {
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final String MONGO_DB = "local";
    private static final String MONGO_COLLECTION = "oplog.rs";
    private static final String FIELD_TIMESTAMP = "ts";
    private final String name;
    private final OffsetProvider offsetProvider;
    private final MongoClient mongoClient;
    private final MongoCollection<Document> collection;
    final String topic;
    final String DB = "admin";
    String streamName;
    //KinesisProducer kinesis;
    AmazonKinesis amazonKinesis;
    //AmazonKinesis amazonKinesis;
    final int batchSize = 100;
    AmazonS3 s3Client;
    final String bucketName;
    final String fileS3Path = "datalake/mongolargedata/";

    public Tailer(final String name, final String mongoUrl, final OffsetProvider offsetProvider,
                  String topic) {

        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(topic);
        Preconditions.checkNotNull(mongoUrl);
        Preconditions.checkNotNull(offsetProvider);
        this.topic = topic;
        this.name = name;
        this.offsetProvider = offsetProvider;

        // mongoclienturi authentication
        Config config = new Config("seagull.properties");
        List<String> addressList = Arrays.asList(config.get("mongodb.adds").split(","));
        List<ServerAddress> addrs = new ArrayList<>();
        List<MongoCredential> credentials = new ArrayList<>();
        addressList.forEach(s -> {
            ServerAddress serverAddress = new ServerAddress(s, 27017);
            if (StringUtils.isNotEmpty(config.get("mongodb.username"))) {
                MongoCredential credential = MongoCredential.createCredential(config.get("mongodb.username"), DB,
                        config.get("mongodb.password").toCharArray());
                credentials.add(credential);
            }
            addrs.add(serverAddress);
        });

        this.mongoClient = new MongoClient(addrs, credentials);
        this.collection = mongoClient.getDatabase(MONGO_DB).getCollection(MONGO_COLLECTION);

        // initialize kinesis producer
        Config cdcConfig = new Config("cdc.properties");
        String regionName = cdcConfig.get("s3.region.name");
        String kinesisKey = cdcConfig.get("kinesis.key");
        String kinesisSecret = cdcConfig.get("kinesis.secret");
        bucketName = cdcConfig.get("s3.bucket.name");
        streamName = cdcConfig.get("kinesis.stream.name");
        String kRegionName = cdcConfig.get("kinesis.region.name");
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(kinesisKey, kinesisSecret);
        s3Client = AmazonS3ClientBuilder.standard().withCredentials(
                new AWSStaticCredentialsProvider(awsCreds)).withRegion(regionName).build();

        amazonKinesis = AmazonKinesisClientBuilder.standard().withRegion(kRegionName)
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .build();
        logger.info("mongo tailer({}) collection is ready", name);


    }

    @Override
    public void close() throws Exception {
        this.mongoClient.close();
    }

    /**
     * Execute the tailer, get the start time of the tailer based on the offset from Redis OffsetProvider,
     * use it to create a mongo collection cursor, iterate the cursor and put records to Kinesis.
     * ONLY use BsonTimestamp to work with timestamps in MongoDB oplog
     * a. if offset is not present, start from 24 hours ago
     * b. otherwise use the offset as start time
     */
    @Override
    public void run() {
        final Optional<Pair<Integer, Integer>> offset = this.offsetProvider.get();
        final BsonTimestamp startTime;
        if (!offset.isPresent()) {
            logger.info("tailer({})'s offset does not exist, start from yesterday.");
            final int ts = Long.valueOf((System.currentTimeMillis() - DateUtils.MILLIS_PER_DAY) / 1000).intValue();
            startTime = new BsonTimestamp(ts, 0);
        } else {
            final Pair<Integer, Integer> offsetValue = offset.get();
            logger.info("tailer({})'s offset is found, start from {}", this.name, offsetValue);
            startTime = new BsonTimestamp(offsetValue.getLeft(), offsetValue.getRight());

        }
        try {
            final Document query = new Document(FIELD_TIMESTAMP, new Document("$gt", startTime));
            final FindIterable<Document> cursor = collection.find(query).cursorType(CursorType.Tailable);
            final Consumer<Document> consumer = doc -> {
                // filter null oplogs
                if (doc.get("ns") != null && StringUtils.isNotEmpty(doc.get("ns").toString())) {
                    final BsonTimestamp ts = (BsonTimestamp) doc.get(FIELD_TIMESTAMP);
                    // send logs to kinesis
                    String partitionKey = doc.get("ns").toString();
                    try {
                        // add \n, otherwise kinesis is not clear
                        String targetData = doc.toJson();
                        ByteBuffer data = ByteBuffer.wrap(targetData.getBytes("UTF-8"));
                        PutRecordRequest putRecordRequest = new PutRecordRequest();
                        putRecordRequest.setStreamName(streamName);
                        putRecordRequest.setPartitionKey(partitionKey);
                        putRecordRequest.withData(data);
                        amazonKinesis.putRecord(putRecordRequest);


                    } catch (UnsupportedEncodingException e) {
                        logger.error("encoding error in doc {}", doc.toJson(), e);
                    } catch (IllegalArgumentException e) {
                        String fileName = fileS3Path + UUID.randomUUID();
                        File file = writeFile(doc.toJson());
                        s3Client.putObject(new PutObjectRequest(
                                bucketName, fileName, file
                        ));
                        logger.error("unexpected data: {}", doc.toJson(), e);

                    }
                    this.offsetProvider.set(ts.getTime(), ts.getInc());
                }
            };
            cursor.forEach(consumer);
            logger.info("mongo tailer {} starts successfully", this.name);
        } catch (MongoSocketOpenException e) {
            throw new RuntimeException("connect to mongo failed...", e);

        }

    }

    /**
     * @param data
     * @return
     */
    private File writeFile(String data) {
        String filePath = "/tmp/largedataset" +
                UUID.randomUUID();
        File file = new File(filePath);
        if (file.exists()) {
            file.delete();
        }
        try {
            File dir = new File(file.getParent());
            dir.mkdirs();
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(filePath), "UTF-8"
            ));
            out.write(data);
            out.close();

        } catch (IOException e) {
            logger.error("write file failed", e);
        }
        return file;
    }

    /**
     * Here'll walk through some of the config options and create an instance of
     * KinesisProducer, which will be used to put records.
     *
     * @return KinesisProducer instance used to put records.
     */
    private KinesisProducer getKinesisProducer() {
        Config cdcConfig = new Config("cdc.properties");

        String regionName = cdcConfig.get("kinesis.region.name");
        String kinesisKey = cdcConfig.get("kinesis.key");
        String kinesisSecret = cdcConfig.get("kinesis.secret");
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(kinesisKey, kinesisSecret);

        KinesisProducerConfiguration config = new KinesisProducerConfiguration();
        config.setRegion(regionName);
        AWSCredentialsProvider awsCred = new AWSStaticCredentialsProvider(awsCreds);
        config.setCredentialsProvider(awsCred);
        // The maxConnections parameter can be used to control the degree of
        // parallelism when making HTTP requests.
        config.setMaxConnections(5);
        // Set a more generous timeout in case we're on a slow connection.
        config.setRequestTimeout(60000);
        // RecordMaxBufferedTime controls how long records are allowed to wait
        // in the KPL's buffers before being sent. Larger values increase
        // aggregation and reduces the number of Kinesis records put, which can
        // be helpful if you're getting throttled because of the records per
        // second limit on a shard. The default value is set very low to
        // minimize propagation delay, so we'll increase it here to get more
        // aggregation.
        config.setRecordMaxBufferedTime(60 * 1000);
        // Note that if you do pass a Configuration instance, mutating that
        // instance after initializing KinesisProducer has no effect. We do not
        // support dynamic re-configuration at the moment.
        KinesisProducer producer = new KinesisProducer(config);

        return producer;
    }

}

