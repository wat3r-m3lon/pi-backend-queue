package com.piqueue.realtime.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Dead-letter inspection consumer.
 *
 * This listener watches both dead-letter queues (realtime + alert). When a
 * message fails in a primary queue and the broker routes it through the
 * dead-letter exchange (DLX), it lands in the matching dead-letter queue (DLQ).
 * This listener consumes that message and logs the broker-generated x-death
 * headers so we can later inspect why it died, how many times it died, and
 * which queue it originally came from.
 */
@Component
public class DlqInspectorListener {

    private static final Logger log = LoggerFactory.getLogger(DlqInspectorListener.class);

    @RabbitListener(queues = {
            "${pi.rabbit.queue-realtime-dlq}",
            "${pi.rabbit.queue-alert-dlq}"
    })
    public void inspect(
            Message message,
            // x-death can be missing in rare cases, such as manual DLQ publishes; required=false avoids NPEs.
            @Header(name = "x-death", required = false) List<Map<String, Object>> xDeath) {

        String body = new String(message.getBody());
        String arrivedOn = message.getMessageProperties().getConsumerQueue(); // Distinguishes the realtime DLQ from the alert DLQ.

        if (xDeath == null || xDeath.isEmpty()) {
            log.warn("[DLQ] dead message on {} but NO x-death header. body={}", arrivedOn, body);
            return;
        }

        // x-death is a list; the most recent death record is at index 0.
        Map<String, Object> latest = xDeath.get(0);
        Object count       = latest.get("count");          // Number of deaths in the original queue.
        Object reason      = latest.get("reason");          // rejected / expired / maxlen
        Object originQueue = latest.get("queue");           // Original queue, for example q.realtime.reading.
        Object diedAt      = latest.get("time");            // Death timestamp (java.util.Date).
        Object exchange    = latest.get("exchange");        // Original exchange, for example sensors.
        Object routingKeys = latest.get("routing-keys");    // List of routing keys, for example [sensor.reading].

        log.error("[DLQ-FORENSICS] arrivedOn={} reason={} count={} originQueue={} originExchange={} originRoutingKeys={} diedAt={} body={}",
                arrivedOn, reason, count, originQueue, exchange, routingKeys, diedAt, body);

        // A message can die across multiple hops; use this debug line for the full chain.
        log.debug("[DLQ-FORENSICS] full x-death chain = {}", xDeath);
    }
}
