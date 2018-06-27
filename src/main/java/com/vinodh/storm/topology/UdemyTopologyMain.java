package com.vinodh.storm.topology;

import com.vinodh.storm.topology.bolt.MyFirstBolt;
import com.vinodh.storm.topology.bolt.YahooFinanceBolt;
import com.vinodh.storm.topology.spout.MyFirstSpout;
import com.vinodh.storm.topology.spout.YahooFinanceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class UdemyTopologyMain {

    public static void main(String[] args) {
        //Build Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("First-Spout", new MyFirstSpout());
        builder.setBolt("First-Bolt", new MyFirstBolt()).shuffleGrouping("First-Spout");

        StormTopology topology = builder.createTopology();

        //Configuration
        Config config = new Config();
        config.setDebug(true);

        //Submit Topology to Cluster
        LocalCluster cluster = new LocalCluster();

        try {
            cluster.submitTopology("First-Topology", config, topology);
            Thread.sleep(5000);
        } catch ( Exception e) {

        } finally {
            cluster.shutdown();
        }
    }
}
