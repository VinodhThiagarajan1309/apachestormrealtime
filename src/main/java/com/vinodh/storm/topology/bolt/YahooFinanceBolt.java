package com.vinodh.storm.topology.bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.PrintWriter;
import java.util.Map;

public class YahooFinanceBolt extends BaseBasicBolt {

    private PrintWriter writer;

    public void prepare(Map stormConf, TopologyContext topologyContext) {
        String fileName = stormConf.get("fileToWrite").toString();

        try {
            this.writer = new PrintWriter(fileName, "UTF-8");
        } catch (Exception e) {
            throw new RuntimeException("Error Opening File " + fileName);
        }

    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        String symbol = tuple.getValue(0).toString();
        String timeStamp = tuple.getString(1);

        Double price = (Double) tuple.getValueByField("price");
        Double prevClose = (Double) tuple.getValueByField("prev_close");

        Boolean gain = false;

        if(price > prevClose) {
            gain = true;
        }

        basicOutputCollector.emit( new Values(symbol,timeStamp,price,gain));
        writer.println(symbol+","+timeStamp+","+price+","+gain);

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare( new Fields("company", "timestamp", "price", "gain"));
    }

    public void cleanup() {
        writer.close();

    }
}
