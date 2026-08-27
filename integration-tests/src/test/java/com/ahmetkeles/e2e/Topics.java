package com.ahmetkeles.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Raw Kafka access for the tests: producing crafted envelopes and capturing
 * what the services really publish. Each capture uses a throwaway consumer
 * group reading the topic from the beginning; matching is always
 * correlation-scoped (by envelope content), never positional, so leftover
 * records from other tests are irrelevant.
 */
final class Topics {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String bootstrapServers;

    Topics(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    void produce(String topic, String key, String value) {
        Properties properties = new Properties();
        properties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        properties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);

        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(properties)) {
            producer.send(new ProducerRecord<>(topic, key, value)).get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while producing to " + topic, exception);
        } catch (Exception exception) {
            throw new IllegalStateException(
                    "Failed to produce to " + topic, exception);
        }
    }

    /**
     * Blocks until an envelope matching the predicate appears on the topic.
     * {@link KafkaConsumer#poll} is the wait primitive here — no sleeps.
     */
    JsonNode awaitEnvelope(
            String topic,
            String description,
            Predicate<JsonNode> match,
            Duration timeout
    ) {
        try (KafkaConsumer<String, String> consumer = newConsumer()) {
            consumer.subscribe(List.of(topic));

            long deadline = System.currentTimeMillis() + timeout.toMillis();

            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record :
                        consumer.poll(Duration.ofMillis(250))) {
                    JsonNode envelope = parse(record.value());
                    if (envelope != null && match.test(envelope)) {
                        return envelope;
                    }
                }
            }
        }

        fail("Timed out after " + timeout.toSeconds() + "s waiting on topic "
                + topic + " for: " + description);
        return null;
    }

    /**
     * Blocks until a record with the given key appears on the topic and
     * returns it whole, headers included — used to inspect dead-letter
     * records, where the recoverer's headers carry the correlation back to
     * the source topic.
     */
    ConsumerRecord<String, String> awaitRecordWithKey(
            String topic,
            String key,
            Duration timeout
    ) {
        try (KafkaConsumer<String, String> consumer = newConsumer()) {
            consumer.subscribe(List.of(topic));

            long deadline = System.currentTimeMillis() + timeout.toMillis();

            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record :
                        consumer.poll(Duration.ofMillis(250))) {
                    if (key.equals(record.key())) {
                        return record;
                    }
                }
            }
        }

        fail("Timed out after " + timeout.toSeconds() + "s waiting on topic "
                + topic + " for a record with key " + key);
        return null;
    }

    /**
     * Reads the topic exactly to its current end offsets and returns the
     * records carrying the given key. Deterministic and sleep-free: the end
     * offsets are captured up front, so this returns as soon as every
     * partition has been consumed to that point — callers use it for
     * negative assertions ("nothing was dead-lettered for this order")
     * after a positive sync point has proven the pipeline finished.
     */
    List<ConsumerRecord<String, String>> recordsWithKey(
            String topic,
            String key
    ) {
        try (KafkaConsumer<String, String> consumer = newConsumer()) {
            List<org.apache.kafka.common.TopicPartition> partitions =
                    consumer.partitionsFor(topic).stream()
                            .map(info -> new org.apache.kafka.common.TopicPartition(
                                    info.topic(), info.partition()))
                            .toList();

            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            var endOffsets = consumer.endOffsets(partitions);
            List<ConsumerRecord<String, String>> matches =
                    new java.util.ArrayList<>();

            long deadline = System.currentTimeMillis() + 15_000;

            while (System.currentTimeMillis() < deadline) {
                boolean done = partitions.stream().allMatch(partition ->
                        consumer.position(partition)
                                >= endOffsets.get(partition));
                if (done) {
                    return matches;
                }

                for (ConsumerRecord<String, String> record :
                        consumer.poll(Duration.ofMillis(250))) {
                    if (key.equals(record.key())) {
                        matches.add(record);
                    }
                }
            }

            fail("Timed out reading " + topic + " to its end offsets");
            return matches;
        }
    }

    private KafkaConsumer<String, String> newConsumer() {
        Properties properties = new Properties();
        properties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(
                ConsumerConfig.GROUP_ID_CONFIG, "e2e-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        return new KafkaConsumer<>(properties);
    }

    static Predicate<JsonNode> envelopeFor(UUID aggregateId, String eventType) {
        return envelope ->
                envelope.hasNonNull("aggregateId")
                        && envelope.hasNonNull("eventType")
                        && aggregateId.toString()
                                .equals(envelope.get("aggregateId").asText())
                        && eventType.equals(envelope.get("eventType").asText());
    }

    private static JsonNode parse(String value) {
        try {
            return MAPPER.readTree(value);
        } catch (Exception exception) {
            return null;
        }
    }
}
