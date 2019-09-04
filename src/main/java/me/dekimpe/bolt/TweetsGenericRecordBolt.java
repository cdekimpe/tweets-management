/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.dekimpe.bolt;

import java.util.Map;
import me.dekimpe.types.Tweet;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 *
 * @author cdekimpe
 */
public class TweetsGenericRecordBolt extends BaseRichBolt {
    
    private OutputCollector outputCollector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("genericRecordTweet"));
    }

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
    
    private void process(Tuple input) {
        Tweet tweet = (Tweet) input.getValueByField("tweet");
        Schema schema = ReflectData.get().getSchema(tweet.getClass());
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("date", tweet.getDate());
        avroRecord.put("text", tweet.getText());
        avroRecord.put("hashtags", tweet.getHashtags());
        outputCollector.emit(new Values(avroRecord));
    }
    
}
