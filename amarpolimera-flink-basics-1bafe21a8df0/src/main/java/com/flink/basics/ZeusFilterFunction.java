package com.flink.basics;

import org.apache.flink.api.common.functions.FilterFunction;

public class ZeusFilterFunction implements FilterFunction<ZeusEvent> {

    private final long eventThresholdCount;

    public ZeusFilterFunction(long eventThresholdCount) {
        this.eventThresholdCount = eventThresholdCount;
    }

    @Override
    public boolean filter(ZeusEvent event)  {
        return event.getEventsCount() < this.eventThresholdCount;
    }
}
