/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.dekimpe.bolt;

import java.net.InetAddress;
import java.util.Map;
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
    public void declareOutputFields(OutputFieldsDeclarer ofd) {   
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
        
        String json;        
        for (Tuple input : inputWindow.get()) {
            json = "{\"date\": " + input.getStringByField("date") + ", "
                    + "\"text\": " + input.getStringByField("text") + ", "
                    + "\"hashtags\": \"" + input.getValueByField("hashtags") + "\"}";
        }
        
    }
    
    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
