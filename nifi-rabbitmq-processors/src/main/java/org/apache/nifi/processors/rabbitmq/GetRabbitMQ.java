package org.apache.nifi.processors.rabbitmq;

import com.rabbitmq.client.*;
import org.apache.nifi.annotation.documentation.SeeAlso;
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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processors.rabbitmq.util.RabbitMQFactory;
import org.apache.nifi.processors.rabbitmq.util.RabbitMQMessage;
import org.apache.nifi.processors.rabbitmq.util.RabbitMQMessageConsumer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@CapabilityDescription("Fetches messages from RabbitMQ")
@Tags({"RabbitMQ", "Get", "Ingest", "Topic", "PubSub", "AMQP"})
@SeeAlso(PutRabbitMQ.class)
public class GetRabbitMQ extends AbstractProcessor {

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Success relationship")
            .build();

    private final BlockingQueue<RabbitMQMessage> rabbitMQMessageQueue = new LinkedBlockingQueue<>();

    private Connection connection;
    private Channel channel;

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

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>(1);
        relationships.add(SUCCESS);
        return relationships;
    }

    @OnScheduled
    public void createConsumers(final ProcessContext context) {
        final String rabbitQueue = context.getProperty(RABBITMQ_QUEUE).getValue();

        try {
            connection = RabbitMQFactory.createConnection(context);
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
            channel.basicConsume(rabbitQueue, true, new RabbitMQMessageConsumer(channel, rabbitMQMessageQueue));
        } catch (ShutdownSignalException sse) {
            getLogger().error("Error consuming RabbitMQ channel[ShutdownSignalException]: {}", new Object[]{sse});
        } catch (Exception e) {
            getLogger().error("Error consuming RabbitMQ channel: {}", new Object[]{e});
            e.printStackTrace();
        }

    }

    @OnStopped
    public void stopConsumer() {
        try {
            RabbitMQFactory.closeConnection(connection, channel);
        } catch (Exception e) {
            getLogger().error("Error closing RabbitMQ connection: {}", new Object[]{e});
            e.printStackTrace();
        }
    }

    @Override
    public void onTrigger(ProcessContext context, final ProcessSession session) throws ProcessException {
        getLogger().info("onTrigger rabbitMQMessageQueue size: " + rabbitMQMessageQueue.size());
        final RabbitMQMessage rabbitMQMessage = rabbitMQMessageQueue.poll();
        if (rabbitMQMessage == null) {
            return;
        }

        final String rabbitQueue = context.getProperty(RABBITMQ_QUEUE).getValue();
        final long start = System.nanoTime();


        FlowFile flowFile = session.create();
        try {
            flowFile = session.write(flowFile,
                    new OutputStreamCallback() {
                        @Override
                        public void process(final OutputStream out) throws IOException {
                            out.write(rabbitMQMessage.getBody());
                        }
                    });

            if (flowFile.getSize() == 0L) {
                session.remove(flowFile);
            } else {
                final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                session.getProvenanceReporter().receive(flowFile, "rabbitmq://" + rabbitQueue, "Received RabbitMQ RabbitMQMessage", millis);
                getLogger().info("Successfully received {} ({}) from RabbitMQ in {} millis", new Object[]{flowFile, flowFile.getSize(), millis});
                session.transfer(flowFile, SUCCESS);
            }
        } catch (Exception e) {
            session.remove(flowFile);
            throw e;
        }
    }
}