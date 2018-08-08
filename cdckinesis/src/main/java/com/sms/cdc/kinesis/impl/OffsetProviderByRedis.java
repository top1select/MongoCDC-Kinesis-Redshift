package com.sms.cdc.kinesis.impl;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.sms.cdc.kinesis.mongo.OffsetProvider;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import scala.Int;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

public class OffsetProviderByRedis implements OffsetProvider {
    private static final Logger logger = LoggerFactory.getLogger(OffsetProviderByRedis.class);

    Jedis jedis;
    private static final Pattern PATTERN;

    static {
        PATTERN = Pattern.compile("\\s*[0-9]+\\s*:\\s*[0-9]+\\s*");
    }

    private final String redisKey;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private Pair<Integer, Integer> latest;

    public OffsetProviderByRedis(String host, int port, String redisKey) {
        jedis = new Jedis(host, port);
        this.redisKey = redisKey;

    }

    private void processCurrentOffset(final Pair<Integer, Integer> pair) {
        try {
            final String offsetStr = pair.getLeft() + ":" + pair.getRight();
            this.jedis.set(redisKey, offsetStr);
            logger.info("output offset {}: {}", this.redisKey, pair);

        } catch (Exception e) {
            logger.error("output offset failed", this.redisKey, pair, Throwables.getStackTraceAsString(e));
            throw Throwables.propagate(e);
        }

    }

    @Override
    public Optional<Pair<Integer, Integer>> get() {
        try {
            if (this.jedis.exists(redisKey) == null) {
                return Optional.empty();
            }
            final String offsetStr = this.jedis.get(redisKey);
            if (StringUtils.isEmpty(offsetStr)) {
                return Optional.empty();
            }
            if (!PATTERN.matcher(offsetStr).matches()) {
                return Optional.empty();
            }
            final String[] arr = StringUtils.split(offsetStr, ":");
            final int left = NumberUtils.toInt(StringUtils.strip(arr[0]));
            final int right = NumberUtils.toInt(StringUtils.strip(arr[1]));
            return Optional.of(Pair.of(left, right));
        } catch (Exception e) {
            logger.error("try to get offset pair failed.", e);
            throw Throwables.propagate(e);
        }

    }
    @Override
    public Optional<Integer> getNormal() {
        try {
            if (this.jedis.exists(redisKey) == null) {
                return Optional.empty();
            }
            final String offsetStr = this.jedis.get(redisKey);
            if (StringUtils.isEmpty(offsetStr)) {
                return Optional.empty();
            }
            final String[] arr = StringUtils.split(offsetStr, ":");
            final int left = NumberUtils.toInt(StringUtils.strip(arr[0]));
            return Optional.of(left);
        } catch (Exception e) {
            logger.error("get offset pair attempt failed", e);
            throw Throwables.propagate(e);
        }

    }

    @Override
    public void set(int timestamp, int version) {
        this.set(Pair.of(timestamp, version));
    }

    @Override
    public void set(int timestamp) {
        this.set(Pair.of(timestamp, 0));
    }

    @Override
    public void set(Pair<Integer, Integer> pair) {
        if (pair == null) {
            return;
        }
        this.processCurrentOffset(pair);

    }

    @Override
    public void close() throws IOException {
        if (this.isClosed.getAndSet(true)) {
            return;
        }
        this.jedis.close();
    }

}
