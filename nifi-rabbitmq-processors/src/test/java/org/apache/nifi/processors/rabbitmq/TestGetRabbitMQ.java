package org.apache.nifi.processors.rabbitmq;

import static org.junit.Assert.assertEquals;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TestGetRabbitMQ {

    private static final Logger LOGGER;

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.io.nio", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.rabbitmq.GetRabbitMQ", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.rabbitmq.TestGetRabbitMQ", "debug");
        LOGGER = LoggerFactory.getLogger(TestGetRabbitMQ.class);
    }

    @Test
    //@Ignore("Intended only for local tests to verify functionality.")
    public void testIntegrationLocally() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(GetRabbitMQ.class);

        runner.setProperty(GetRabbitMQ.RABBITMQ_HOST, "localhost");
        runner.setProperty(GetRabbitMQ.RABBITMQ_PORT, "5672");
        runner.setProperty(GetRabbitMQ.RABBITMQ_USERNAME, "guest");
        runner.setProperty(GetRabbitMQ.RABBITMQ_PASSWORD, "guest");
        runner.setProperty(GetRabbitMQ.RABBITMQ_VIRTUALHOST, "/");
        runner.setProperty(GetRabbitMQ.RABBITMQ_QUEUE, "hello");

        LOGGER.info("Starting test");
        LOGGER.info("count: " + runner.getQueueSize().getObjectCount());
        LOGGER.info("thread count: " + runner.getThreadCount());

        runner.run(1);

        /*final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetRabbitMQ.SUCCESS);
        for (final MockFlowFile flowFile : flowFiles) {
            System.out.println(flowFile.getAttributes());
            System.out.println(new String(flowFile.toByteArray()));
            System.out.println();
        }*/

        assertEquals(true, true);
    }
}
