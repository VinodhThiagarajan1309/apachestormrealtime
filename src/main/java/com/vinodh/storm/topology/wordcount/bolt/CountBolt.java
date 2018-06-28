package com.vinodh.storm.topology.wordcount.bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class CountBolt extends BaseBasicBolt {

    Integer id;
    String name;
    Map<String,Integer> counters;
    String fileName;

    public void prepare(Map stormConf, TopologyContext context) {
        this.counters = new HashMap<>();
        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();
        this.fileName = stormConf.get("dirToWrite").toString() +
                "output" +
                context.getThisTaskId() +
                "-" +
                context.getThisComponentId() +
                ".txt";
    }


    @Override
    public void execute(Tuple input, BasicOutputCollector basicOutputCollector) {

        String word = input.getStringByField("word");

        if(!counters.containsKey(word)) {
            counters.put(word,1);
        } else {
            counters.put(word, counters.get(word)+1);
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public void cleanup()  {

        try {
            PrintWriter writer = new PrintWriter(this.fileName, "UTF-8");
            for (String word : counters.keySet()) {
                writer.println(word + "-" + counters.get(word));
            }
            writer.close();
        } catch (Exception e) {

        } finally {
        }
    }
}
