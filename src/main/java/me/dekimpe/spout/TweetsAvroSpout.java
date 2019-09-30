/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.dekimpe.spout;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import me.dekimpe.avro.value.Tweet;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 *
 * @author Coreuh
 */
public class TweetsAvroSpout extends BaseRichSpout {
    private KafkaConsumer<me.dekimpe.avro.key.Tweet, Tweet> consumer;
    private SpoutOutputCollector outputCollector;

    @Override
    public void open(Map map, TopologyContext tc, SpoutOutputCollector soc) {
        try {
            outputCollector = soc;
            Properties config = new Properties();
            config.put("client.id", java.net.InetAddress.getLocalHost().getHostName());
            config.put("group.id", "speed-layer");
            config.put("bootstrap.servers", "confluent-kafka:9092");
            config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
            config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
            config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://192.168.10.20:8081");
            consumer = new KafkaConsumer<>(config);
            consumer.subscribe(Collections.singletonList("tweet"));
        } catch (UnknownHostException e) {
            System.err.println(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

    @Override
    public void nextTuple() {
        ConsumerRecords<me.dekimpe.avro.key.Tweet, Tweet> records = consumer.poll(1000);
        for (ConsumerRecord<me.dekimpe.avro.key.Tweet, Tweet> record : records) {
            Tweet tweet = record.value();
            outputCollector.emit(new Values(tweet));
        }
    }

}
