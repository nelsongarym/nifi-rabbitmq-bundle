package org.apache.nifi.processors.rabbitmq.util;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Queue;

public class RabbitMQMessageConsumer extends DefaultConsumer {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMQMessageConsumer.class);

    private final Queue<RabbitMQMessage> queue;

    public RabbitMQMessageConsumer(Channel channel, Queue<RabbitMQMessage> queue) {
        super(channel);
        this.queue = queue;
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        logger.info("Got message: " + Arrays.toString(body));
        queue.add(new RabbitMQMessage(consumerTag, envelope, properties, body));
        logger.info("OnScheduled messageQueue size: " + queue.size());
    }
}
