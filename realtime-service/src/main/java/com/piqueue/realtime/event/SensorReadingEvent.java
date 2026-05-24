package com.piqueue.realtime.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.OffsetDateTime;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public record SensorReadingEvent(
        Integer schemaVersion,
        String eventType,
        String deviceId,
        OffsetDateTime recordedAt,
        Double temperatureC,
        Double humidityPct,
        Double pressureHpa,
        Double lightLux,
        Double noiseLevel,
        String source,
        Map<String, Object> metadata
) {
}
