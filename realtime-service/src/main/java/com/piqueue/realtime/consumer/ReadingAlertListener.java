package com.piqueue.realtime.consumer;

import com.piqueue.realtime.event.SensorReadingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class ReadingAlertListener {

    private static final Logger log = LoggerFactory.getLogger(ReadingAlertListener.class);

    @RabbitListener(queues = "${pi.rabbit.queue-alert}")
    public void onMessage(SensorReadingEvent event) {
        log.info("alert <- device={} t={} noise={} recordedAt={}",
                event.deviceId(), event.temperatureC(), event.noiseLevel(), event.recordedAt());
        // TODO: rule evaluation (ThresholdRule / StaleRule / SpikeRule) -> Slack notification
        // Failures should propagate so listener retry routes exhausted attempts to DLQ.
    }
}
