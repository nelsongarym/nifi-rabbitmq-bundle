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

import static org.apache.nifi.processors.rabbitmq.util.RabbitMQProperties.RABBITMQ_HOST;
import static org.apache.nifi.processors.rabbitmq.util.RabbitMQProperties.RABBITMQ_PORT;
import static org.apache.nifi.processors.rabbitmq.util.RabbitMQProperties.RABBITMQ_USERNAME;
import static org.apache.nifi.processors.rabbitmq.util.RabbitMQProperties.RABBITMQ_PASSWORD;
import static org.apache.nifi.processors.rabbitmq.util.RabbitMQProperties.RABBITMQ_VIRTUALHOST;
import static org.apache.nifi.processors.rabbitmq.util.RabbitMQProperties.RABBITMQ_QUEUE;

import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processors.rabbitmq.util.Message;
import org.apache.nifi.processors.rabbitmq.util.MessageConsumer;

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

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Success relationship")
            .build();

    private final BlockingQueue<Message> messageQueue = new LinkedBlockingQueue<>();
    private Set<Relationship> relationships;

    private Connection connection;
    private Channel channel;

    @Override
    public void init(final ProcessorInitializationContext context){
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(RABBITMQ_HOST);
        props.add(RABBITMQ_PORT);
        props.add(RABBITMQ_USERNAME);
        props.add(RABBITMQ_PASSWORD);
        props.add(RABBITMQ_VIRTUALHOST);
        props.add(RABBITMQ_QUEUE);

        return props;
    }

    @OnScheduled
    public void createConsumers(final ProcessContext context) {
        getLogger().info("OnScheduled");

        final String rabbitQueue = context.getProperty(RABBITMQ_QUEUE).getValue();

        Config config = new Config();

        try {
            connection = createRabbitMQConnection(config, context);
        } catch (Exception e) {
            getLogger().error("Error creating RabbitMQ connection: {}", new Object[]{e});
            return;
        }

        try {
            channel = connection.createChannel();
            channel.queueDeclare(rabbitQueue, true, false, false, null);
        } catch (Exception e) {
            getLogger().error("Error creating RabbitMQ channel: {}", new Object[]{e});
            return;
        }

        try {
            getChannel().basicConsume(rabbitQueue, true, new MessageConsumer(channel, messageQueue));
        } catch (ShutdownSignalException sse) {
            getLogger().error("Error consuming RabbitMQ channel[ShutdownSignalException]: {}", new Object[]{sse});
        } catch (Exception e) {
            getLogger().error("Error consuming RabbitMQ channel: {}", new Object[]{e});
            e.printStackTrace();
        }

    }

    @OnStopped
    public void stopConsumer() {
        getLogger().info("OnStopped");

        this.close();
    }

    @Override
    public void onTrigger(ProcessContext context, final ProcessSession session) throws ProcessException {
        getLogger().info("onTrigger messageQueue size: " + messageQueue.size());
        final Message message = messageQueue.poll();
        if (message == null) {
            return;
        }

        final String rabbitQueue = context.getProperty(RABBITMQ_QUEUE).getValue();
        final long start = System.nanoTime();
        FlowFile flowFile = session.create();
        try {
            flowFile = session.write(flowFile,
                    new OutputStreamCallback() {
                        @Override
                        public void process(OutputStream outputStream) throws IOException {
                            try (final OutputStream out = new BufferedOutputStream(outputStream, 65536)) {
                                out.write(message.getBody());
                            }
                        }
                    });

            if (flowFile.getSize() == 0L) {
                session.remove(flowFile);
            } else {
                final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                session.getProvenanceReporter().receive(flowFile, "rabbitmq://" + rabbitQueue, "Received RabbitMQ Message", millis);
                getLogger().info("Successfully received {} ({}) from RabbitMQ in {} millis", new Object[]{flowFile, flowFile.getSize(), millis});
                session.transfer(flowFile, SUCCESS);
            }
        } catch (Exception e) {
            session.remove(flowFile);
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

    private Connection createRabbitMQConnection(Config config, final ProcessContext context) throws IOException, TimeoutException {

        final String rabbitHost = context.getProperty(RABBITMQ_HOST).getValue();
        final int rabbitPort = context.getProperty(RABBITMQ_PORT).asInteger();
        final String rabbitVirtualHost = context.getProperty(RABBITMQ_VIRTUALHOST).getValue();
        final String rabbitUsername = context.getProperty(RABBITMQ_USERNAME).getValue();
        final String rabbitPassword = context.getProperty(RABBITMQ_PASSWORD).getValue();


        config = config.withRecoveryPolicy(RecoveryPolicies.recoverAlways())
                .withRetryPolicy(new RetryPolicy()
                        .withMaxAttempts(200)
                        .withInterval(Duration.seconds(1))
                        .withMaxDuration(Duration.minutes(5)));

        ConnectionOptions options = new ConnectionOptions()
                .withHost(rabbitHost)
                .withPort(rabbitPort)
                .withVirtualHost(rabbitVirtualHost)
                .withUsername(rabbitUsername)
                .withPassword(rabbitPassword);

        getLogger().info("Creating connection: " + config.toString());

        return Connections.create(options, config);
    }

    @Override
    public Set<Relationship> getRelationships(){
        return relationships;
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