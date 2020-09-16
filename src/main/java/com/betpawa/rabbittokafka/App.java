package com.betpawa.rabbittokafka;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class App {

    public static void main(String[] args) throws IOException, TimeoutException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {

        final String queueToConsume = getConfiguration().get("RABBIT_TO_KAFKA_RABBIT_QUEUE");
        final String topicToProduce = getConfiguration().get("RABBIT_TO_KAFKA_KAFKA_TOPIC");
        final Connection rabbitConnection = getConnectionFactoryFromEnv().newConnection();
        final Channel rabbitChannel = rabbitConnection.createChannel();
        final KafkaProducer<String, byte[]> kafkaProducer = getKafkaProducerFromEnv();
        final boolean autoAck = false;
        final AtomicLong counter = new AtomicLong();
        final AtomicLong noOffsetCounter = new AtomicLong();
        final long messagesToProcess = Long.parseLong(getConfiguration().get("RABBIT_TO_KAFKA_MESSAGES_TO_PROCESS"));

        rabbitChannel.basicConsume(queueToConsume, autoAck, new DefaultConsumer(rabbitChannel) {
            @Override
            public void handleDelivery(final String consumerTag,
                                       final Envelope envelope,
                                       final AMQP.BasicProperties properties,
                                       final byte[] body) {
                try {

                    if (counter.get() >= messagesToProcess) {
                        System.err.printf("%d messages processed. Terminating", counter.get());
                        System.exit(0);
                    }

                    final Future<RecordMetadata> recordMetadataFuture = handleConsumedMessage(kafkaProducer, topicToProduce, body);
                    if (recordMetadataFuture.get().hasOffset()) {
                        counter.incrementAndGet();
                    } else {
                        noOffsetCounter.incrementAndGet();
                    }
                    rabbitChannel.basicAck(envelope.getDeliveryTag(), false);

                } catch (Exception e) {
                    System.err.printf("Cannot handle consumed message: %s%n", Base64.getEncoder().encodeToString(body));
                    e.printStackTrace();
                }
            }
        });
    }

    private static ConnectionFactory getConnectionFactoryFromEnv() throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        final ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri(getConfiguration().get("RABBIT_TO_KAFKA_RABBIT_URI"));
        return connectionFactory;
    }

    private static Future<RecordMetadata> handleConsumedMessage(final KafkaProducer<String, byte[]> kafkaProducer,
                                                                final String topicToProduce,
                                                                final byte[] body) {
        return kafkaProducer.send(new ProducerRecord<>(topicToProduce, body));
    }

    private static KafkaProducer<String, byte[]> getKafkaProducerFromEnv() throws IOException {
        final String producerPropertiesPath = getConfiguration().get("RABBIT_TO_KAFKA_KAFKA_PRODUCER_PROPERTIES");
        final Properties properties = new Properties();
        properties.load(new FileInputStream(producerPropertiesPath));
        return new KafkaProducer<>(properties, new StringSerializer(), new ByteArraySerializer());
    }

    private static Map<String, String> getConfiguration() {
//        return Map.of(
//                "RABBIT_TO_KAFKA_KAFKA_PRODUCER_PROPERTIES", "kafka_producer.properties",
//                "RABBIT_TO_KAFKA_RABBIT_URI", "amqp://guest:guest@rabbitmq:5672",
//                "RABBIT_TO_KAFKA_RABBIT_QUEUE", "rn.tanzania.betslips",
//                "RABBIT_TO_KAFKA_KAFKA_TOPIC", "regulatory-notification-event",
//                "RABBIT_TO_KAFKA_MESSAGES_TO_PROCESS", "10"
//        );
        return Map.of(
                "RABBIT_TO_KAFKA_KAFKA_PRODUCER_PROPERTIES", Objects.requireNonNull(System.getenv("RABBIT_TO_KAFKA_KAFKA_PRODUCER_PROPERTIES")),
                "RABBIT_TO_KAFKA_RABBIT_URI", Objects.requireNonNull(System.getenv("RABBIT_TO_KAFKA_RABBIT_URI")),
                "RABBIT_TO_KAFKA_RABBIT_QUEUE", Objects.requireNonNull(System.getenv("RABBIT_TO_KAFKA_RABBIT_QUEUE")),
                "RABBIT_TO_KAFKA_KAFKA_TOPIC", Objects.requireNonNull(System.getenv("RABBIT_TO_KAFKA_KAFKA_TOPIC")),
                "RABBIT_TO_KAFKA_MESSAGES_TO_PROCESS", Optional.ofNullable(System.getenv("RABBIT_TO_KAFKA_MESSAGES_TO_PROCESS")).orElse("10")
        );
    }
}
