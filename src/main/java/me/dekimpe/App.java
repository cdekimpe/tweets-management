package me.dekimpe;

import me.dekimpe.bolt.TweetsParsingBolt;
import me.dekimpe.bolt.TweetsSpeedLayerBolt;
import me.dekimpe.spout.TweetsAvroSpout;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
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
        
        builder.setSpout("tweets", new TweetsAvroSpout());
        
        builder.setBolt("speed-layer", new TweetsSpeedLayerBolt().withTumblingWindow(BaseWindowedBolt.Count.of(100)))
                .shuffleGrouping("tweets");
        
        StormTopology topology = builder.createTopology();
        Config config = new Config();
        config.setNumWorkers(4);
        config.setMessageTimeoutSecs(7200);
    	String topologyName = "Tweets-Management";

        StormSubmitter.submitTopology(topologyName, config, topology);
    }
}
