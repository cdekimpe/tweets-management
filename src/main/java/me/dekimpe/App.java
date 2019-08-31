package me.dekimpe;

import me.dekimpe.bolt.*;

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
            InvalidTopologyException, AuthorizationException
    {
        TopologyBuilder builder = new TopologyBuilder();
        
        KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder = KafkaSpoutConfig.builder("kafka1:9092", "tests")
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "batch-layer");
    	KafkaSpoutConfig<String, String> spoutConfig = spoutConfigBuilder.build();
    	builder.setSpout("tweets-spout", new KafkaSpout<String, String>(spoutConfig));
        
        builder.setBolt("tweets-parsed", new TweetsParsingBolt())
                .shuffleGrouping("tweets-spout");
        
        builder.setBolt("save-tweets", new TweetsSaveBolt().withTumblingWindow(BaseWindowedBolt.Duration.of(1000 * 60)))
                .shuffleGrouping("tweets-parsed");
        
        StormTopology topology = builder.createTopology();
        Config config = new Config();
        config.setNumWorkers(4);
        //config.setMaxSpoutPending(200);
        config.setMessageTimeoutSecs(120);
    	String topologyName = "Tweets-Management";
        
        StormSubmitter.submitTopology(topologyName, config, topology);
    }
}
