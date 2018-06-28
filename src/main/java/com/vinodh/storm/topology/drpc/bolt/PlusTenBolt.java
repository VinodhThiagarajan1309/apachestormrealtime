package com.vinodh.storm.topology.drpc.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class PlusTenBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector basicOutputCollector) {

        Integer inputInt = Integer.parseInt(input.getString(1));
        Integer output = inputInt + 10;
        basicOutputCollector.emit( new Values(input.getValue(0), output));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare( new Fields("id" , "result"));

    }
}
