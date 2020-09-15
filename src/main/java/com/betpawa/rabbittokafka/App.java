package com.betpawa.rabbittokafka;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class App {

    public static void main(String[] args) throws IOException, TimeoutException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {

        final String queueToConsume = Objects.requireNonNull(System.getenv("RABBIT_TO_KAFKA_RABBIT_QUEUE"));
        final String topicToProduce = Objects.requireNonNull(System.getenv("RABBIT_TO_KAFKA_KAFKA_TOPIC"));
        final Connection rabbitConnection = getConnectionFactoryFromEnv().newConnection();
        final Channel rabbitChannel = rabbitConnection.createChannel();
        final KafkaProducer<byte[], byte[]> kafkaProducer = getKafkaProducerFromEnv();
        final boolean autoAck = false;
        final AtomicLong counter = new AtomicLong();

        rabbitChannel.basicConsume(queueToConsume, autoAck, new DefaultConsumer(rabbitChannel) {
            @Override
            public void handleDelivery(final String consumerTag,
                                       final Envelope envelope,
                                       final AMQP.BasicProperties properties,
                                       final byte[] body) {
                try {
                    final Future<RecordMetadata> recordMetadataFuture = handleConsumedMessage(kafkaProducer, topicToProduce, body);
                    if (recordMetadataFuture.get().hasOffset()) {
                        System.err.printf("Sent messages: %s%n", counter.incrementAndGet());
                    }
                } catch (Exception e) {
                    System.err.printf("Cannot handle consumed message: %s%n", Base64.getEncoder().encodeToString(body));
                    e.printStackTrace();
                }
            }
        });
    }

    private static ConnectionFactory getConnectionFactoryFromEnv() throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        final ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri(Objects.requireNonNull(System.getenv("RABBIT_TO_KAFKA_RABBIT_URI")));
        return connectionFactory;
    }

    private static Future<RecordMetadata> handleConsumedMessage(final KafkaProducer<byte[], byte[]> kafkaProducer,
                                                                final String topicToProduce,
                                                                final byte[] body) {
        return kafkaProducer.send(new ProducerRecord<>(topicToProduce, body));
    }

    private static KafkaProducer<byte[], byte[]> getKafkaProducerFromEnv() throws IOException {
        final String producerPropertiesPath = Objects.requireNonNull(System.getenv("RABBIT_TO_KAFKA_KAFKA_PRODUCER_PROPERTIES"));
        final Properties properties = new Properties();
        properties.load(new FileInputStream(producerPropertiesPath));
        return new KafkaProducer<>(properties, new ByteArraySerializer(), new ByteArraySerializer());
    }
}
