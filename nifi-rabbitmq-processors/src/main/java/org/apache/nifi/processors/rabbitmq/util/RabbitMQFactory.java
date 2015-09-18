package org.apache.nifi.processors.rabbitmq.util;

import net.jodah.lyra.ConnectionOptions;
import net.jodah.lyra.Connections;
import net.jodah.lyra.config.Config;
import net.jodah.lyra.config.RecoveryPolicies;
import net.jodah.lyra.config.RetryPolicy;
import net.jodah.lyra.util.Duration;
import org.apache.nifi.processor.ProcessContext;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.apache.nifi.processors.rabbitmq.util.RabbitMQProperties.*;

public class RabbitMQFactory {

    public static Connection createConnection(Config config, final ProcessContext context) throws IOException, TimeoutException {

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

        return Connections.create(options, config);
    }

    public static void closeConnection(Connection connection, Channel channel) throws IOException, TimeoutException {
        if (channel.isOpen()) {
            channel.close();
        }
        if (connection.isOpen()) {
            connection.close();
        }
    }
}
