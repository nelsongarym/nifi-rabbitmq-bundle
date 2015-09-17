package org.apache.nifi.processors.rabbitmq.util;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.util.StandardValidators;

public class Properties {
    public static final PropertyDescriptor RABBITMQ_HOST = new PropertyDescriptor.Builder()
            .name("RabbitMQ Host")
            .description("RabbitMQ host")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("localhost")
            .expressionLanguageSupported(false)
            .build();

    public static final PropertyDescriptor RABBITMQ_PORT = new PropertyDescriptor.Builder()
            .name("RabbitMQ port")
            .description("RabbitMQ port")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .defaultValue("5672")
            .expressionLanguageSupported(false)
            .build();

    public static final PropertyDescriptor RABBITMQ_USERNAME = new PropertyDescriptor.Builder()
            .name("RabbitMQ username")
            .description("RabbitMQ username")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("guest")
            .expressionLanguageSupported(false)
            .build();

    public static final PropertyDescriptor RABBITMQ_PASSWORD = new PropertyDescriptor.Builder()
            .name("RabbitMQ password")
            .description("RabbitMQ password")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("guest")
            .expressionLanguageSupported(false)
            .build();

    public static final PropertyDescriptor RABBITMQ_VIRTUALHOST = new PropertyDescriptor.Builder()
            .name("RabbitMQ virtual host")
            .description("RabbitMQ virtual host")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("/")
            .expressionLanguageSupported(false)
            .build();

    public static final PropertyDescriptor RABBITMQ_QUEUE = new PropertyDescriptor.Builder()
            .name("RabbitMQ queue")
            .description("RabbitMQ queue")
            .required(false)
            .addValidator(Validator.VALID)
            .defaultValue("")
            .expressionLanguageSupported(false)
            .build();
}
