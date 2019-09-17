/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.dekimpe.bolt;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import me.dekimpe.config.ElasticSearch;
import me.dekimpe.avro.value.Tweet;

/**
 *
 * @author Coreuh
 */
public class TweetsSpeedLayerBolt extends BaseWindowedBolt {
    
    private OutputCollector outputCollector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        
    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        outputCollector = oc;
    }

    @Override
    public void execute(TupleWindow window) {
        try {
            process(window);
        } catch (Exception e) {
            System.err.println(e);
        }
    }
    
    private void process(TupleWindow window) throws UnknownHostException {
        
        // Create a connection to ES cluster
        Settings settings = Settings.builder()
                .put("cluster.name", ElasticSearch.CLUSTER_NAME)
                .put("client.transport.sniff", "true").build();
        
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ElasticSearch.HOST1), ElasticSearch.PORT))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ElasticSearch.HOST2), ElasticSearch.PORT))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ElasticSearch.HOST3), ElasticSearch.PORT));
        
        String json;
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (Tuple input : window.get()) {
            Tweet tweet = (Tweet) input.getValueByField("tweet");
            for (String hashtag : tweet.getHashtags()) {
                json = "{\"timestamp\": " + (tweet.getTimestamp() * 1000) + ", \"hashtag\": \"" + hashtag + "\"}";
                System.out.println(json);
                bulkRequest.add(client.prepareIndex(ElasticSearch.INDEX, "hashtags")
                    .setSource(json, XContentType.JSON));
            }
        }

        BulkResponse bulkResponse = bulkRequest.get();
        for (Tuple input : window.get()) {
            if (bulkResponse.hasFailures()) {
                outputCollector.fail(input);
            } else {
                outputCollector.ack(input);
            }
        }

        client.close();
    }
}
