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
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.bolt.AvroGenericRecordBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.Partitioner;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.hdfs.common.Partitioner;
/*import org.apache.storm.hdfs.bolt.AvroGenericRecordBolt;
import org.apache.storm.hdfs.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.sync.SyncPolicy;
import org.apache.storm.hdfs.sync.CountSyncPolicy;
import org.apache.storm.hdfs.format.FileNameFormat;
import org.apache.storm.hdfs.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.format.RecordFormat;
import org.apache.storm.hdfs.common.Partitioner;*/
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
        
        // sync the filesystem after every 1k tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        // rotate files when they reach 5MB
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
            }
        };

        AvroGenericRecordBolt bolt = new AvroGenericRecordBolt()
                .withFsUrl("hdfs://hdfs-namenode:9000")
                .withFileNameFormat(fileNameFormat)
                .withRotationPolicy(rotationPolicy)
                .withPartitioner(partitoner);
                //.withSyncPolicy(syncPolicy)
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }
    
}
