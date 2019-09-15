/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.dekimpe.bolt;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author cdekimpe
 */
public class TweetsParsingBolt extends BaseRichBolt  {
    
    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        outputCollector = oc;
    }

    @Override
    public void execute(Tuple input) {
        try {
            process(input);
            outputCollector.ack(input);
        } catch (Exception e) {
            e.printStackTrace();
            outputCollector.fail(input);
        }
    }
    
    // Input example : {"timestamp": 1563961571, "eur": 8734.6145}
    private void process(Tuple input) throws ParseException, JSONException {
        JSONObject obj = new org.json.JSONObject(input.getStringByField("value"));
        
        // Getting text from the tweet, no parsing needed
        String text = (String) obj.get("text");
        
        // Parsing List of String for hashtags
        List<String> hashtags = new ArrayList<>();
        JSONArray jArray = obj.getJSONArray("hashtags");
        if (jArray != null) {
            for (int i = 0; i < jArray.length(); i++) {
                hashtags.add(jArray.getString(i));
            }
        }
        
        /* Setting all the elements to the Tweet Type Object
        Tweet tweet = new Tweet();
        tweet.setText(text);
        tweet.setDate((String) obj.get("date"));
        tweet.setHashtags(hashtags);

        outputCollector.emit(new Values(tweet));
        outputCollector.ack(input);*/
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }
    
}
