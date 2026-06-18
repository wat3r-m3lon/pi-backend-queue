package com.piqueue.realtime.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(RabbitProperties.class)
public class RabbitTopologyConfig {

    private final RabbitProperties props;

    public RabbitTopologyConfig(RabbitProperties props) {
        this.props = props;
    }

    @Bean
    public TopicExchange sensorsExchange() {
        return new TopicExchange(props.exchange(), true, false);
    }

    @Bean
    public TopicExchange sensorsDlxExchange() {
        return new TopicExchange(props.dlx(), true, false);
    }

    @Bean
    public Queue realtimeReadingQueue() {
        return QueueBuilder.durable(props.queueRealtime())
                .withArgument("x-dead-letter-exchange", props.dlx())
                .withArgument("x-dead-letter-routing-key", props.routingKeyReadingDead())
                .build();
    }

    @Bean
    public Queue alertReadingQueue() {
        return QueueBuilder.durable(props.queueAlert())
                .withArgument("x-dead-letter-exchange", props.dlx())
                .withArgument("x-dead-letter-routing-key", props.routingKeyAlertDead())
                .build();
    }

    @Bean
    public Queue alertReadingDlq() {
        return QueueBuilder.durable(props.queueAlertDlq()).build();
    }

    @Bean
    public Queue realtimeReadingDlq() {
        return QueueBuilder.durable(props.queueRealtimeDlq()).build();
    }

    @Bean
    public Binding realtimeBinding(Queue realtimeReadingQueue, TopicExchange sensorsExchange) {
        return BindingBuilder.bind(realtimeReadingQueue).to(sensorsExchange).with(props.routingKeyReading());
    }

    @Bean
    public Binding alertBinding(Queue alertReadingQueue, TopicExchange sensorsExchange) {
        return BindingBuilder.bind(alertReadingQueue).to(sensorsExchange).with(props.routingKeyReading());
    }

    @Bean
    public Binding alertDlqBinding(Queue alertReadingDlq, TopicExchange sensorsDlxExchange) {
        return BindingBuilder.bind(alertReadingDlq).to(sensorsDlxExchange).with(props.routingKeyAlertDead());
    }

    @Bean
    public Binding realtimeDlqBinding(Queue realtimeReadingDlq, TopicExchange sensorsDlxExchange) {
        return BindingBuilder.bind(realtimeReadingDlq).to(sensorsDlxExchange).with(props.routingKeyReadingDead());
    }

    @Bean
    public MessageConverter jsonMessageConverter() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.findAndRegisterModules();
        return new Jackson2JsonMessageConverter(mapper);
    }
}
