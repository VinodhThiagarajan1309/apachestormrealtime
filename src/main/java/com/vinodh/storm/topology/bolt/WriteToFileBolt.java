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

public class WriteToFileBolt extends BaseBasicBolt {

    private PrintWriter writer;

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        String str = tuple.getStringByField("integer") + "-" + tuple.getStringByField("bucket");
        basicOutputCollector.emit(new Values(str));
        writer.println(str);

        //basicOutputCollector.emit( new Values( tuple.getString(1),tuple.getString(2)));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare( new Fields("field"));
    }

    public void prepare(Map stormConf, TopologyContext context) {

        String fileName = "output" + context.getThisTaskId() + "-" + context.getThisComponentId() + ".txt";
        try {
            this.writer = new PrintWriter(stormConf.get("dirToWrite").toString()+fileName, "UTF-8");
        } catch (Exception e) {

        }
    }

    public void cleanup() {
        this.writer.close();
    }
}
