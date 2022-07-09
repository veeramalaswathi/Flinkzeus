package com.flink.basics;

import org.apache.flink.api.java.functions.KeySelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

    public class ZeusRawKeyByFunction implements KeySelector<ZeusEvent, String> {

        private static final Logger log = LoggerFactory.getLogger(ZeusRawKeyByFunction.class);

        @Override
        public String getKey(ZeusEvent event) throws Exception {
            String key = String.format("%s,%s,%s,%s",
                    event.getUserId(),
                    event.getEventSessionID(),
                    event.getUniqueDeviceID(),
                    event.getPlaybackSessionID());

            log.debug("Key in Key Selector = {} ", key);
            return key;
        }
    }

