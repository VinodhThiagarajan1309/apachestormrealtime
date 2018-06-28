package com.vinodh.storm.topology.wordcount.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitBolt extends BaseBasicBolt {

    /**
     * Convert single line into words
     * @param input
     * @param basicOutputCollector
     */
    public void execute(Tuple input, BasicOutputCollector basicOutputCollector) {

        String[] words = input.getStringByField("line").split(" ");
        for(String word : words) {
            word = word.trim();
            if(!word.isEmpty()) {
                word = word.toLowerCase();
                basicOutputCollector.emit( new Values(word));
            }
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));

    }
}
