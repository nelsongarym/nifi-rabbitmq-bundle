package org.apache.nifi.processors.rabbitmq;

import com.rabbitmq.client.*;
import net.jodah.lyra.ConnectionOptions;
import net.jodah.lyra.Connections;
import net.jodah.lyra.config.Config;
import net.jodah.lyra.config.RecoveryPolicies;
import net.jodah.lyra.config.RetryPolicy;
import net.jodah.lyra.util.Duration;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.io.OutputStreamCallback;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@SupportsBatching
@CapabilityDescription("Fetches messages from RabbitMQ")
@Tags({"RabbitMQ", "Get", "Ingest", "Topic", "PubSub", "AMQP"})
public class GetRabbitMQ extends AbstractProcessor {

    private final BlockingQueue<AMQPMessage> messageQueue = new LinkedBlockingQueue<>();
    private Set<Relationship> relationships;

    private Connection connection;
    private Channel channel;

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Success relationship")
            .build();

    @Override
    public void init(final ProcessorInitializationContext context){
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @OnScheduled
    public void createConsumers(final ProcessContext context) {
        getLogger().info("OnScheduled");

        Config config = new Config();

        try {
            connection = createRabbitMQConnection(config);
        } catch (Exception e) {
            getLogger().error("Error creating RabbitMQ connection: {}", new Object[]{e});
            return;
        }

        try {
            channel = connection.createChannel();
        } catch (Exception e) {
            getLogger().error("Error creating RabbitMQ channel: {}", new Object[]{e});
            return;
        }

        DefaultConsumer consumer;

        consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                                       final Envelope envelope,
                                       final AMQP.BasicProperties properties,
                                       final byte[] body) throws IOException {

                getLogger().info("Got message: " + body.toString());
                messageQueue.add(new AMQPMessage(consumerTag, envelope, properties, body));
            }
        };

        try {
            getChannel().basicConsume("hello", true, consumer);
        } catch (ShutdownSignalException sse) {
            getLogger().error("Error consuming RabbitMQ channel[ShutdownSignalException]: {}", new Object[]{sse});
        } catch (Exception e) {
            getLogger().error("Error consuming RabbitMQ channel: {}", new Object[]{e});
            e.printStackTrace();
            return;
        }

    }

    @OnStopped
    public void stopConsumer() {
        getLogger().info("OnStopped");

        this.close();
    }

    @Override
    public void onTrigger(ProcessContext processContext, final ProcessSession processSession) throws ProcessException {
        final AMQPMessage message = messageQueue.poll();
        if (message == null) {
            return;
        }

        final long start = System.nanoTime();
        FlowFile flowFile = processSession.create();
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("queue", "hello");

        try {
            flowFile = processSession.write(flowFile,
                    new OutputStreamCallback() {
                        @Override
                        public void process(OutputStream outputStream) throws IOException {
                            try (final OutputStream out = new BufferedOutputStream(outputStream, 65536)) {
                                out.write(message.getBody());
                            }
                        }
                    });
            processSession.putAllAttributes(flowFile, attributes);
            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            processSession.getProvenanceReporter().receive(flowFile, "rabbitmq://hello", "Received RabbitMQ Message", millis);
            getLogger().info("Successfully received {} from RabbitMQ in {} millis", new Object[]{flowFile, millis});
            processSession.transfer(flowFile, SUCCESS);
        } catch (Exception e) {
            processSession.remove(flowFile);
            throw e;
        }
    }

    private void close() {
        try {
            if (channel != null) {
                channel.close();
                channel = null;
            }
            if (connection != null) {
                connection.close();
                connection = null;
            }
        } catch (Exception e) {
            getLogger().error("Error cleanly closing RabbitMQ connection: {}", new Object[]{e});
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

        getLogger().info("Creating connection: " + config.toString());

        return Connections.create(options, config);
    }

    @Override
    public Set<Relationship> getRelationships(){
        return relationships;
    }

    private static class AMQPMessage {
        private final String consumerTag;
        private final Envelope envelope;
        private final AMQP.BasicProperties properties;
        private final byte[] body;

        public AMQPMessage(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
            this.consumerTag = consumerTag;
            this.envelope = envelope;
            this.properties = properties;
            this.body = body;
        }

        public String getConsumerTag() {
            return consumerTag;
        }

        public Envelope getEnvelope() {
            return envelope;
        }

        public AMQP.BasicProperties getProperties() {
            return properties;
        }

        public byte[] getBody() {
            return body;
        }

    }

    private Channel getChannel() {
        final Connection c = connection;

        if (c == null) {
            throw new IllegalStateException("Client has not yet been initialized");
        }

        final Channel ch = channel;

        if (ch == null) {
            throw new IllegalStateException("Channel has not yet been initialized");
        }

        return ch;
    }
}