package me.dekimpe.types;

import java.io.Serializable;
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
    
    public String toString() {
        String result =  "{\"text\": \"" + text + "\", \"date\": \"" + date + "\", \"hashtags\": [";
        for (String hashtag : hashtags) {
            result += '"' + hashtag + "\", ";
        }
        if (hashtags != null)
            result = result.substring(0, result.length() - 2);
        result += "]}";
        return result;
    }
    
}
