package com.flink.basics;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileInitialMap implements MapFunction<String, Tuple1<String>> {

    private static final Logger log = LoggerFactory.getLogger(FileInitialMap.class);
    @Override
    public Tuple1<String> map(String s) throws Exception {
        //log.info("Input JSON Received : {}",s);
        return new Tuple1<>(s);
    }
}
