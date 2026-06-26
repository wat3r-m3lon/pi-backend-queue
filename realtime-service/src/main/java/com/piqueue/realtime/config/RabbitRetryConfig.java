package com.piqueue.realtime.config;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.config.RabbitRetryTemplateCustomizer;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.policy.SimpleRetryPolicy;

import java.util.HashMap;
import java.util.Map;

/**
 * Custom consumer-side retry classification: decides which exceptions should
 * be retried and which ones should fail fast into the DLQ.
 *
 * Context: application.yml enables client-side retry (max-attempts=3 with
 * backoff), but YAML only controls the attempt count and interval. It cannot
 * define which exception classes are non-retryable. Exception-level
 * classification uses RabbitRetryTemplateCustomizer, Spring Boot's official
 * hook for customizing the RetryTemplate used by the auto-configured listener
 * container factory.
 *
 * Classification rules:
 *  - MessageConversionException (bad JSON / wrong field type) -> no retry, immediate DLQ
 *  - AmqpRejectAndDontRequeueException (business-defined permanent failure) -> no retry, immediate DLQ
 *  - All other exceptions (transient failures, such as downstream timeouts) -> retryable
 *    using the 3 attempts + backoff from application.yml
 */
@Configuration
public class RabbitRetryConfig {

    @Bean
    public RabbitRetryTemplateCustomizer retryPolicyCustomizer() {
        return (target, retryTemplate) -> {

            // key = exception class, value = retryable flag (true=retry / false=no retry)
            Map<Class<? extends Throwable>, Boolean> retryableExceptions = new HashMap<>();
            // Format/poison-frame errors: retrying the same bad bytes will not fix them.
            retryableExceptions.put(MessageConversionException.class, false);
            // Business-defined permanent failures. With retry enabled, this exception would
            // otherwise be retried 3 times, so mark it false to make "throw it" mean
            // "send to the DLQ immediately without retrying".
            retryableExceptions.put(AmqpRejectAndDontRequeueException.class, false);

            // Replacing the RetryPolicy loses max-attempts from YAML because that value
            // lives inside the policy object. Keep this in sync with application.yml.
            int maxAttempts = 3;

            SimpleRetryPolicy policy = new SimpleRetryPolicy(
                    maxAttempts,
                    retryableExceptions,
                    // Third argument, traverseCauses=true: inspect the cause chain.
                    // The real exception is often wrapped by ListenerExecutionFailedException;
                    // without this, the inner MessageConversionException would not match.
                    true,
                    // Fourth argument, defaultValue=true: exceptions not listed above are
                    // retryable by default. This preserves retries for transient failures
                    // such as ordinary RuntimeException. Setting false would turn this into
                    // whitelist mode and disable transient retries too.
                    true);

            // Replace only the classification policy; YAML backoff settings
            // (initial-interval/multiplier/max-interval) still apply.
            retryTemplate.setRetryPolicy(policy);
        };
    }
}
