package com.piqueue.realtime.state;

import com.piqueue.realtime.event.SensorReadingEvent;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class LatestReadingCache {

    private final ConcurrentHashMap<String, SensorReadingEvent> store = new ConcurrentHashMap<>();

    public void put(SensorReadingEvent event) {
        if (event == null || !StringUtils.hasText(event.deviceId())) {
            return;
        }
        store.compute(event.deviceId(), (deviceId, current) -> isNewerOrEqual(event, current) ? event : current);
    }

    public SensorReadingEvent get(String deviceId) {
        return store.get(deviceId);
    }

    public Collection<SensorReadingEvent> snapshot() {
        return List.copyOf(store.values());
    }

    private static boolean isNewerOrEqual(SensorReadingEvent candidate, SensorReadingEvent current) {
        if (current == null || current.recordedAt() == null) {
            return true;
        }
        if (candidate.recordedAt() == null) {
            return false;
        }
        return !candidate.recordedAt().isBefore(current.recordedAt());
    }
}
