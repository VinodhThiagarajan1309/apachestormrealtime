package com.vinodh.storm.topology;

import com.vinodh.storm.topology.bolt.YahooFinanceBolt;
import com.vinodh.storm.topology.spout.YahooFinanceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {

    public static void main(String[] args) {

        //Build Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Yahoo-Finance-Spout", new YahooFinanceSpout());
        builder.setBolt("Yahoo-Finance-Bolt", new YahooFinanceBolt()).shuffleGrouping("Yahoo-Finance-Spout");

        StormTopology topology = builder.createTopology();

        //Configuration
        Config config = new Config();
        config.setDebug(true);
        config.put("fileToWrite","/Users/vthiagarajan/Documents/WorkSpaces/VinodhWorkSpaces/yf-output/output.txt");

        //Submit Topology to Cluster
        LocalCluster cluster = new LocalCluster();

        try {
            cluster.submitTopology("Stock-Tracker-Topology", config, topology);
            Thread.sleep(10000);
        } catch ( Exception e) {

        } finally {
            cluster.shutdown();
        }

    }
}
