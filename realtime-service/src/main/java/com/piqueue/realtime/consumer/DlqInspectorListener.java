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
 * 死信尸检消费者。
 *
 * 它同时监听两个死信队列（realtime + alert）。任何一条消息在主队列处理失败、
 * 被 broker 投递到死信交换机（DLX）后，最终会落到对应的死信队列（DLQ），
 * 由这个 listener 接住，并把 broker 自动写入的 x-death 头信息打成日志，
 * 方便事后查“这条消息为什么死了、死了几次、原本来自哪个队列”。
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
            // x-death 极少数情况下可能缺失（例如直接往 DLQ 手动发消息），required=false 防 NPE
            @Header(name = "x-death", required = false) List<Map<String, Object>> xDeath) {

        String body = new String(message.getBody());
        String arrivedOn = message.getMessageProperties().getConsumerQueue(); // 区分是 realtime 还是 alert 的 DLQ

        if (xDeath == null || xDeath.isEmpty()) {
            log.warn("[DLQ] dead message on {} but NO x-death header. body={}", arrivedOn, body);
            return;
        }

        // x-death 是一个列表；最近一次死亡记录在第 0 个元素
        Map<String, Object> latest = xDeath.get(0);
        Object count       = latest.get("count");          // 在原队列死过几次
        Object reason      = latest.get("reason");          // rejected / expired / maxlen
        Object originQueue = latest.get("queue");           // 原队列，如 q.realtime.reading
        Object diedAt      = latest.get("time");            // 死亡时间（java.util.Date）
        Object exchange    = latest.get("exchange");        // 原 exchange，如 sensors
        Object routingKeys = latest.get("routing-keys");    // 列表，如 [sensor.reading]

        log.error("[DLQ-FORENSICS] arrivedOn={} reason={} count={} originQueue={} originExchange={} originRoutingKeys={} diedAt={} body={}",
                arrivedOn, reason, count, originQueue, exchange, routingKeys, diedAt, body);

        // 消息可能多跳死亡，需要完整链路时看这行（debug 级别）
        log.debug("[DLQ-FORENSICS] full x-death chain = {}", xDeath);
    }
}
