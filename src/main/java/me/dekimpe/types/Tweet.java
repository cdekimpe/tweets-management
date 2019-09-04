package me.dekimpe.types;

//import org.apache.storm.hdfs.avro.GenericAvroSerializer;
import java.io.Serializable;
import java.util.Date;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.storm.hdfs.avro.AbstractAvroSerializer;

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
    
    private Date date;
    private String text;
    private List<String> hashtags;
    
    public Tweet() {}

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
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
        String result =  "{\"text\": \"" + text + "\", \"date\": \"" + date.getTime() + "\", \"hashtags\": [";
        for (String hashtag : hashtags) {
            result += '"' + hashtag + "\", ";
        }
        if (hashtags.size() > 0)
            result = result.substring(0, result.length() - 2);
        result += "]}";
        return result;
    }
}
