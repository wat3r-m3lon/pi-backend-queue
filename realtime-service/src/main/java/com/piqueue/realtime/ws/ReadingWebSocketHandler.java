package com.piqueue.realtime.ws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.piqueue.realtime.event.SensorReadingEvent;
import com.piqueue.realtime.state.LatestReadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;

@Component
public class ReadingWebSocketHandler extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(ReadingWebSocketHandler.class);

    private final ReadingBroadcaster broadcaster;
    private final LatestReadingCache cache;
    private final ObjectMapper objectMapper;

    public ReadingWebSocketHandler(ReadingBroadcaster broadcaster,
                                   LatestReadingCache cache,
                                   ObjectMapper objectMapper) {
        this.broadcaster = broadcaster;
        this.cache = cache;
        this.objectMapper = objectMapper;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws IOException {
        broadcaster.register(session);
        log.info("ws connected: {}", session.getId());
        for (SensorReadingEvent event : cache.snapshot()) {
            try {
                session.sendMessage(new TextMessage(objectMapper.writeValueAsString(event)));
            } catch (JsonProcessingException e) {
                log.warn("failed to send snapshot to {}", session.getId(), e);
            }
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        broadcaster.unregister(session);
        log.info("ws closed: {} ({})", session.getId(), status);
    }
}
