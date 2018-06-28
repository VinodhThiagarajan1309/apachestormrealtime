package com.vinodh.storm.topology.wordcount.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

public class WordCountSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private boolean completed = false;
    private FileReader fileReader;
    private String str;
    private BufferedReader reader;

    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try {
            this.fileReader = new FileReader(conf.get("FileToRead").toString());
        } catch (FileNotFoundException e) {
            throw new RuntimeException(" Error reading file [ " + conf.get("wordFile") + "]" );
        }

        this.collector = spoutOutputCollector;
        this.reader = new BufferedReader(fileReader);
    }

    public void nextTuple() {

        if(!completed) {
            try {
                this.str = reader.readLine();

                if(null != this.str) {
                    this.collector.emit(new Values(str));
                } else {
                    completed = true;
                    fileReader.close();
                }
            } catch (Exception e) {

                throw new RuntimeException(" Error reading Tuple", e);

            }

        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare( new Fields("line"));

    }
}