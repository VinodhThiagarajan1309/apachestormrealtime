package com.vinodh.storm.topology.wordcount;

import com.vinodh.storm.topology.wordcount.bolt.CountBolt;
import com.vinodh.storm.topology.wordcount.bolt.SplitBolt;
import com.vinodh.storm.topology.wordcount.spout.WordCountSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopology {

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("WordCountSpout",new WordCountSpout());
        builder.setBolt("SplitBolt", new SplitBolt()).shuffleGrouping("WordCountSpout");
        builder.setBolt("CountBolt", new CountBolt(),2).fieldsGrouping("SplitBolt", new Fields("word"));

        StormTopology topology = builder.createTopology();

        // Configuration
        Config config = new Config();
        config.setDebug(true);
        config.put("FileToRead","/Users/vthiagarajan/Documents/WorkSpaces/VinodhWorkSpaces/word-count-input/sample.txt");
        config.put("dirToWrite","/Users/vthiagarajan/Documents/WorkSpaces/VinodhWorkSpaces/word-count-output/");


        // Cluster setup
        LocalCluster localCluster = new LocalCluster();

        try {
            localCluster.submitTopology("Word-Count-Topology", config, builder.createTopology());
            Thread.sleep(30000);
        } catch (InterruptedException e) {

        }
        finally {
            localCluster.shutdown();
        }

    }
}
