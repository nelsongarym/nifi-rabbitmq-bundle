package org.apache.nifi.processors.rabbitmq.util;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Queue;

public class MessageConsumer extends DefaultConsumer {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumer.class);

    private Channel channel;
    private Queue<Message> queue;

    public MessageConsumer(Channel channel, Queue<Message> queue) {
        super(channel);
        this.channel = channel;
        this.queue = queue;
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        logger.info("Got message: " + body.toString());
        queue.add(new Message(consumerTag, envelope, properties, body));
        logger.info("OnScheduled messageQueue size: " + queue.size());
    }
}