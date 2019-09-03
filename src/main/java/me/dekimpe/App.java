package me.dekimpe;

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
import org.apache.storm.hdfs.bolt.AvroGenericRecordBolt;
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
        
        SyncPolicy syncPolicy = new CountSyncPolicy(100);
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(64.0f, FileSizeRotationPolicy.Units.MB);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withExtension(".avro")
                .withPath("/tweets/");      
        Partitioner partitoner = new Partitioner() {
            public String getPartitionPath(Tuple tuple) {
                Tweet tweet = (Tweet) tuple.getValueByField("tweet");
                int year = tweet.getDate().getYear();
                int month = tweet.getDate().getMonth();
                int day = tweet.getDate().getDay();
                int hour = tweet.getDate().getHours();
                return Path.SEPARATOR + year + Path.SEPARATOR + month + Path.SEPARATOR + day + Path.SEPARATOR + hour;
        }};
        AvroGenericRecordBolt bolt = new AvroGenericRecordBolt()
                .withFsUrl("hdfs://hdfs-namenode:9000")
                .withFileNameFormat(fileNameFormat)
                .withRotationPolicy(rotationPolicy)
                .withPartitioner(partitoner)
                .withSyncPolicy(syncPolicy);
        
        builder.setBolt("batch-layer", bolt).shuffleGrouping("tweets-parsed");
        
        StormTopology topology = builder.createTopology();
        Config config = new Config();
        config.setNumWorkers(4);
        //config.setMaxSpoutPending(200);
        config.setMessageTimeoutSecs(7200);
    	String topologyName = "Tweets-Management";
        
        StormSubmitter.submitTopology(topologyName, config, topology);
    }
}
