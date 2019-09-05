package me.dekimpe;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import me.dekimpe.bolt.*;
import me.dekimpe.types.Tweet;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.avro.AvroUtils;
import org.apache.storm.hdfs.bolt.AvroGenericRecordBolt;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.Partitioner;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws AlreadyAliveException,
            InvalidTopologyException, AuthorizationException, Exception
    {
        TopologyBuilder builder = new TopologyBuilder();
        
        KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder = KafkaSpoutConfig.builder("kafka1:9092", "tests")
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "batch-layer");
    	KafkaSpoutConfig<String, String> spoutConfig = spoutConfigBuilder.build();
    	builder.setSpout("tweets-spout", new KafkaSpout<String, String>(spoutConfig));
        
        builder.setBolt("tweets-parsed", new TweetsParsingBolt())
                .shuffleGrouping("tweets-spout");
        
        builder.setBolt("speed-layer", new TweetsSpeedLayerBolt().withTumblingWindow(BaseWindowedBolt.Count.of(1000)))
                .shuffleGrouping("tweets-parsed");
        
        builder.setBolt("tweets-avro-records", new TweetsGenericRecordBolt())
                .shuffleGrouping("tweets-parsed");
        
        SyncPolicy syncPolicy = new CountSyncPolicy(100);
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(64.0f, FileSizeRotationPolicy.Units.MB);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withExtension(".avro")
                .withPath("/tweets/");      
        Partitioner partitoner = new Partitioner() {
            public String getPartitionPath(Tuple tuple) {
                Date date = new Date(tuple.getIntegerByField("date"));
                int year = date.getYear();
                int month = date.getMonth();
                int day = date.getDay();
                int hour = date.getHours();
                return Path.SEPARATOR + year + Path.SEPARATOR + month + Path.SEPARATOR + day + Path.SEPARATOR + hour;
        }};
        HdfsBolt bolt = new HdfsBolt()
                .withFsUrl("hdfs://hdfs-namenode:9000")
                .withFileNameFormat(fileNameFormat)
                .withRotationPolicy(rotationPolicy)
                .withPartitioner(partitoner)
                .withSyncPolicy(syncPolicy);
        
        builder.setBolt("batch-layer", bolt).shuffleGrouping("tweets-parsed");
        
        StormTopology topology = builder.createTopology();
        Config config = new Config();
        config.setNumWorkers(4);
        config.registerSerialization(Tweet.class);
        //config.setMaxSpoutPending(200);
        config.setMessageTimeoutSecs(7200);
        AvroUtils.addAvroKryoSerializations(config);
    	String topologyName = "Tweets-Management";
        
        AvroUtils.addAvroKryoSerializations(config);
        StormSubmitter.submitTopology(topologyName, config, topology);
    }
}
