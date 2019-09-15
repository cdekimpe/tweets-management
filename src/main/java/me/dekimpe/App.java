package me.dekimpe;

import me.dekimpe.bolt.TweetsParsingBolt;
import me.dekimpe.bolt.TweetsSpeedLayerBolt;
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
        
        KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder = KafkaSpoutConfig.builder("confluent-kakfa:9092", "tweets")
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "speed-layer");
    	KafkaSpoutConfig<String, String> spoutConfig = spoutConfigBuilder.build();
    	builder.setSpout("tweets-spout", new KafkaSpout<String, String>(spoutConfig));
        
        builder.setBolt("tweets-parsed", new TweetsParsingBolt())
                .shuffleGrouping("tweets-spout");
        
        builder.setBolt("speed-layer", new TweetsSpeedLayerBolt().withTumblingWindow(BaseWindowedBolt.Count.of(100)))
                .shuffleGrouping("tweets-parsed");
        
        StormTopology topology = builder.createTopology();
        Config config = new Config();
        config.setNumWorkers(4);
        config.setMessageTimeoutSecs(7200);
    	String topologyName = "Tweets-Management";

        StormSubmitter.submitTopology(topologyName, config, topology);
    }
}
