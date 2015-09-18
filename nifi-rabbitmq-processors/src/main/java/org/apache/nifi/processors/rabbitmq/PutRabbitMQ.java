package org.apache.nifi.processors.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import net.jodah.lyra.config.Config;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processors.rabbitmq.util.RabbitMQFactory;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processors.rabbitmq.util.RabbitMQProperties.RABBITMQ_QUEUE;

@CapabilityDescription("Fetches messages from RabbitMQ")
@Tags({"RabbitMQ", "Put", "Ingest", "Topic", "PubSub", "AMQP"})
@SeeAlso(GetRabbitMQ.class)
public class PutRabbitMQ extends AbstractProcessor {

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Success relationship")
            .build();

    private Connection connection;
    private Channel channel;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return super.getSupportedPropertyDescriptors();
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>(1);
        relationships.add(SUCCESS);
        return relationships;
    }

    @OnScheduled
    public void createProducers(final ProcessContext context) {
        final String rabbitQueue = context.getProperty(RABBITMQ_QUEUE).getValue();

        Config config = new Config();

        try {
            connection = RabbitMQFactory.createConnection(config, context);
        } catch (Exception e) {
            getLogger().error("Error creating RabbitMQ connection: {}", new Object[]{e});
            return;
        }

        try {
            channel = connection.createChannel();
            channel.queueDeclare(rabbitQueue, true, false, false, null);
        } catch (Exception e) {
            getLogger().error("Error creating RabbitMQ channel: {}", new Object[]{e});
        }
    }

    @OnStopped
    public void closeProducer() {
        try {
            RabbitMQFactory.closeConnection(connection, channel);
        } catch (Exception e) {
            getLogger().error("Error closing RabbitMQ connection: {}", new Object[]{e});
            e.printStackTrace();
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            final long start = System.nanoTime();
            final String rabbitQueue = context.getProperty(RABBITMQ_QUEUE).getValue();
            final byte[] message = new byte[(int) flowFile.getSize()];

            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
                    StreamUtils.fillBuffer(in, message, true);
                }
            });

            try {
                channel.basicPublish("", rabbitQueue, null, message);
                final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                session.getProvenanceReporter().send(flowFile, "rabbitmq://" + rabbitQueue, "Received RabbitMQ RabbitMQMessage", millis);
                getLogger().info("Successfully received {} ({}) from RabbitMQ in {} millis", new Object[]{flowFile, flowFile.getSize(), millis});
                session.transfer(flowFile, SUCCESS);
            } catch (Exception e) {
                getLogger().error("Failed to send {} to RabbitMQ due to {}; routing to failure", new Object[] { flowFile, e });
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

    }


}