package me.dekimpe.types;

//import org.apache.storm.hdfs.avro.GenericAvroSerializer;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Coreuh
 */
public class Tweet implements Serializable {
    
    private String date;
    private String text;
    private int timestamp;
    private List<String> hashtags;
    
    public Tweet() {}

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }
    
    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }
    
    public int getTimestamp() {
        return timestamp;
    }
    
    public Date getDateObj() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
        return sdf.parse(date);
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public List<String> getHashtags() {
        return hashtags;
    }

    public void setHashtags(List<String> hashtags) {
        this.hashtags = hashtags;
    }
    
    @Override
    public String toString() {
        String result =  "{\"text\": \"" + text + "\", \"date\": \"" + timestamp + "\", \"hashtags\": [";
        for (String hashtag : hashtags) {
            result += '"' + hashtag + "\", ";
        }
        if (hashtags.size() > 0)
            result = result.substring(0, result.length() - 2);
        result += "]}";
        return result;
    }
}

