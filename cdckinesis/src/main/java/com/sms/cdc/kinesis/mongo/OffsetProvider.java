package com.sms.cdc.kinesis.mongo;

import org.apache.commons.lang3.tuple.Pair;

import java.io.Closeable;
import java.util.Optional;

public interface OffsetProvider extends AutoCloseable, Closeable {

    Optional<Pair<Integer, Integer>> get();

    Optional<Integer> getNormal();

    void set(int timestamp, int version);

    void set(int timestamp);

    void set(Pair<Integer, Integer> timestampAndVersion);

}

