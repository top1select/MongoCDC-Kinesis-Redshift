package com.sms.cdc.kinesis.impl;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.sms.cdc.kinesis.mongo.OffsetProvider;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

public class OffsetProviderByZK implements OffsetProvider {

    private static final Logger logger = LoggerFactory.getLogger(OffsetProviderByZK.class);

    private static final Pattern PATTERN;
    private static final int DEQUE_SIZE;

    static {
        PATTERN = Pattern.compile("\\s*[0-9]+\\s*:\\s*[0-9]+\\s*");
        DEQUE_SIZE = 1000;
    }

    private final CuratorFramework zookeeper;
    private final String path;
    private final BlockingDeque<Pair<Integer, Integer>> queue;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final boolean autoCloseZK;
    private Pair<Integer, Integer> latest;

    public OffsetProviderByZK(CuratorFramework zookeeper, String path, boolean autoCloseZK) {
        this.autoCloseZK = autoCloseZK;
        this.zookeeper = zookeeper;
        this.path = path;
        queue = new LinkedBlockingDeque<>(DEQUE_SIZE);
        ForkJoinPool.commonPool().execute(()->{
            for (; ; ) {
                if (this.isClosed.get()) {
                    break;
                }
                try {
                    final Pair<Integer, Integer> current = this.queue.takeFirst();
                    if (latest == null || isNewer(current)) {
                        this.processCurrentOffset(current);
                        this.latest = current;
                    }
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
                }
        );
    }

    private boolean isNewer(Pair<Integer, Integer> current) {
        final int cl = current.getLeft();
        final int cr = current.getRight();
        final int ll = latest.getLeft();
        final int lr = latest.getRight();
        return cl > ll || (cl == ll && cr == lr);
    }

    private void processCurrentOffset(final Pair<Integer, Integer> pair) {
        try {
            final byte[] bytes = (pair.getLeft() + ":" + pair.getRight()).getBytes(Charsets.UTF_8);
            if (this.zookeeper.checkExists().forPath(this.path) == null) {
                this.zookeeper.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(this.path, bytes);
            } else {
                this.zookeeper.setData().forPath(this.path, bytes);
            }
            logger.info("write offset {}: {}", this.path, pair);
        } catch (Exception e) {
            logger.error("write offset {}:{} failed: {}", this.path, pair, Throwables.getStackTraceAsString(e));
            throw Throwables.propagate(e);
        }
    }
    @Override
    public Optional<Pair<Integer, Integer>> get() {
        try {
            if (this.zookeeper.checkExists().forPath(this.path) == null) {
                return Optional.empty();
            }
            final byte[] bytes = this.zookeeper.getData().forPath(this.path);
            if (ArrayUtils.isEmpty(bytes)) {
                return Optional.empty();
            }
            final String str = new String(bytes, Charsets.UTF_8);
            if (!PATTERN.matcher(str).matches()) {
                return Optional.empty();
            }
            final String[] arr = StringUtils.split(str, ":");
            final int left = NumberUtils.toInt(StringUtils.strip(arr[0]));
            final int right = NumberUtils.toInt(StringUtils.strip(arr[1]));
            return Optional.of(Pair.of(left, right));
        } catch (Exception e) {
            logger.error("try to get offset pair failed.", e);
            throw Throwables.propagate(e);
        }
    }

    @Override
    /**
     * regular timestamp left only
     */
    public Optional<Integer> getNormal() {
        try {
            if (this.zookeeper.checkExists().forPath(this.path) == null) {
                return Optional.empty();
            }
            final byte[] bytes = this.zookeeper.getData().forPath(this.path);
            if (ArrayUtils.isEmpty(bytes)) {
                return Optional.empty();
            }
            final String str = new String(bytes, Charsets.UTF_8);
            if (!PATTERN.matcher(str).matches()) {
                return Optional.empty();
            }
            final String[] arr = StringUtils.split(str, ":");
            final int left = NumberUtils.toInt(StringUtils.strip(arr[0]));
            return Optional.of(left);
        } catch (Exception e) {
            logger.error("try to get offset pair failed.", e);
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void set(int timestamp, int version) {
        this.set(Pair.of(timestamp, version));
    }

    @Override
    /**
     * regular timestamp
     */
    public void set(int timestamp) {
        this.set(Pair.of(timestamp, 0));
    }

    @Override
    public void set(Pair<Integer, Integer> pair) {
        if (pair == null) {
            return;
        }
        if (this.queue.size() == DEQUE_SIZE) {
            this.queue.clear();
        }
        try {
            this.queue.putFirst(pair);
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
    }

    @Override
    public void close() throws IOException {
        if (this.isClosed.getAndSet(true)) {
            return;
        }
        if (this.autoCloseZK) {
            this.zookeeper.close();
        }
    }

}


