package com.piqueue.realtime.consumer;

import com.piqueue.realtime.event.SensorReadingEvent;
import com.piqueue.realtime.state.LatestReadingCache;
import com.piqueue.realtime.ws.ReadingBroadcaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class ReadingRealtimeListener {

    private static final Logger log = LoggerFactory.getLogger(ReadingRealtimeListener.class);

    private final LatestReadingCache cache;
    private final ReadingBroadcaster broadcaster;

    public ReadingRealtimeListener(LatestReadingCache cache, ReadingBroadcaster broadcaster) {
        this.cache = cache;
        this.broadcaster = broadcaster;
    }

    @RabbitListener(queues = "${pi.rabbit.queue-realtime}")
    public void onMessage(SensorReadingEvent event) {
        log.info("realtime <- device={} t={} h={} recordedAt={}",
                event.deviceId(), event.temperatureC(), event.humidityPct(), event.recordedAt());
        cache.put(event);
        broadcaster.broadcast(event);
    }
}
