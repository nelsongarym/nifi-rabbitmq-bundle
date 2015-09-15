package org.apache.nifi.processors.rabbitmq;

import com.rabbitmq.client.*;
import net.jodah.lyra.ConnectionOptions;
import net.jodah.lyra.Connections;
import net.jodah.lyra.config.Config;
import net.jodah.lyra.config.RecoveryPolicies;
import net.jodah.lyra.config.RetryPolicy;
import net.jodah.lyra.util.Duration;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.io.OutputStreamCallback;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeoutException;

@SupportsBatching
@CapabilityDescription("Fetches messages from RabbitMQ")
@Tags({"RabbitMQ", "Get", "Ingest", "Topic", "PubSub", "AMQP"})
public class GetRabbitMQ extends AbstractProcessor {

    private Connection connection;
    private Channel channel;

    @Override
    public void onTrigger(ProcessContext processContext, final ProcessSession processSession) throws ProcessException {
        DefaultConsumer consumer;

        Config config = new Config();

        try {
            connection = createRabbitMQConnection(config);
        } catch (Exception e) {
            getLogger().error("Error creating RabbitMQ connection: {}", e);
            return;
        }

        try {
            channel = connection.createChannel();
        } catch (Exception e) {
            getLogger().error("Error creating RabbitMQ channel: {}", e);
            return;
        }

        consumer = new DefaultConsumer(channel) {
            @Override public void handleDelivery(String consumerTag,
                                                 final Envelope envelope,
                                                 final AMQP.BasicProperties properties,
                                                 final byte[] body) throws IOException {

                FlowFile flowFile = processSession.create();
                try {
                    flowFile = processSession.write(flowFile,
                            new OutputStreamCallback() {
                                @Override
                                public void process(OutputStream outputStream) throws IOException {
                                    try (final OutputStream out = new BufferedOutputStream(outputStream, 65536)) {
                                        out.write(body);
                                    }
                                }
                            });
                } catch (Exception e) {
                    processSession.remove(flowFile);
                    throw e;
                }
            }
        };

        try {
            channel.basicConsume("hello", true, consumer);
        } catch (Exception e) {
            getLogger().error("Error creating RabbitMQ channel: {}", e);
            close();
            return;
        }
    }

    private void close() {
        try {
            channel.close();
            connection.close();
        } catch (Exception e) {
            getLogger().error("Error cleanly closing RabbitMQ connection: {}", e);
        }
    }

    private Connection createRabbitMQConnection(Config config) throws IOException, TimeoutException {

        config = config.withRecoveryPolicy(RecoveryPolicies.recoverAlways())
                .withRetryPolicy(new RetryPolicy()
                        .withMaxAttempts(200)
                        .withInterval(Duration.seconds(1))
                        .withMaxDuration(Duration.minutes(5)));

        ConnectionOptions options = new ConnectionOptions()
                .withHost("localhost")
                .withPort(5672)
                .withVirtualHost("/")
                .withUsername("guest")
                .withPassword("guest");

        return Connections.create(options, config);
    }
}