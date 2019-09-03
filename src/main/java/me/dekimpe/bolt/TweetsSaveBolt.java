/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.dekimpe.bolt;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import me.dekimpe.types.Tweet;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.tuple.Fields;

/**
 *
 * @author cdekimpe
 */
public class TweetsSaveBolt extends BaseWindowedBolt {
    
    private OutputCollector outputCollector;
    
    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        outputCollector = oc;
    }
    
    @Override
    public void execute(TupleWindow inputWindow) {
        try {
            process(inputWindow);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private void process(TupleWindow inputWindow) {
        
        List<Tweet> tweets = new ArrayList<Tweet>();
        for (Tuple input : inputWindow.get()) {
            tweets.add((Tweet) input.getValueByField("tweet"));
            System.out.println((Tweet) input.getValueByField("tweet"));
        }
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }
    
}
