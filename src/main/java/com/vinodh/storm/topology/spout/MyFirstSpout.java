package com.vinodh.storm.topology.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class MyFirstSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    private Integer i = 0;

    /**
     * Initialize the spout
     * @param map
     * @param topologyContext
     * @param spoutOutputCollector
     */
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        this.collector = spoutOutputCollector;

    }

    public void nextTuple() {

        this.collector.emit(new Values(this.i));
        this.i = this.i + 1;

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare( new Fields("field"));
    }

    public void ack(Object msgId) {
    }

    public void fail(Object msgId) {
    }
}
