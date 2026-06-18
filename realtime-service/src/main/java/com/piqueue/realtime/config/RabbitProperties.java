package com.piqueue.realtime.config;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "pi.rabbit")
public record RabbitProperties(
        @NotBlank
        String exchange,
        @NotBlank
        String dlx,
        @NotBlank
        String routingKeyReading,
        @NotBlank
        String queueRealtime,
        @NotBlank
        String queueAlert,
        @NotBlank
        String queueAlertDlq,
        @NotBlank
        String routingKeyAlertDead,
        @NotBlank
        String queueRealtimeDlq,
        @NotBlank
        String routingKeyReadingDead
) {
}
