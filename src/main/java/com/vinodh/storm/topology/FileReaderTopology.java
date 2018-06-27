package com.vinodh.storm.topology;

import com.vinodh.storm.topology.bolt.FileReaderSimpleBolt;
import com.vinodh.storm.topology.spout.FileReaderSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class FileReaderTopology {

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("File-Reader-Spout", new FileReaderSpout());
        builder.setBolt("File-Reader-Bolt", new FileReaderSimpleBolt()).shuffleGrouping("File-Reader-Spout");

        // Configuration
        Config config = new Config();
        config.setDebug(true);
        config.put("FileToRead","/Users/vthiagarajan/Documents/WorkSpaces/VinodhWorkSpaces/yf-output/sample.txt");
        config.put("dirToWrite","/Users/vthiagarajan/Documents/WorkSpaces/VinodhWorkSpaces/yf-output/");


        // Cluster setup
        LocalCluster localCluster = new LocalCluster();

        try {
            localCluster.submitTopology("File-Reader-Topology", config, builder.createTopology());
            Thread.sleep(4000);
        } catch (InterruptedException e) {

        }
            finally {
            localCluster.shutdown();
        }

    }
}
