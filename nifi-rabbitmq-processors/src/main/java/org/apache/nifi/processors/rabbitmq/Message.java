package org.apache.nifi.processors.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

public class Message {
    private final String consumerTag;
    private final Envelope envelope;
    private final AMQP.BasicProperties properties;
    private final byte[] body;

    public Message(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
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
