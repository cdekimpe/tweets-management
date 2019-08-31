/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.dekimpe.bolt;

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
        
        Tweet tweet;      
        for (Tuple input : inputWindow.get()) {
            String text = input.getStringByField("text");
            Date date = (Date) input.getValueByField("date");
            List<String> hashtags = (List<String>) input.getValueByField("hashtags");
            tweet = new Tweet();
            tweet.setText(text);
            tweet.setDate(date);
            tweet.setHashtags(hashtags);
            System.out.println(tweet);
        }
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {   
    }
    
}
