package org.apache.nifi.processors.rabbitmq;

import org.junit.BeforeClass;
import org.apache.log4j.BasicConfigurator;

public class TestGetRabbitMQ {
    @BeforeClass
    public static void configureLogging() {
        System.setProperty("org.slf4j.simpleLogger.log.kafka", "INFO");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.processors.kafka", "INFO");
        BasicConfigurator.configure();
    }
}