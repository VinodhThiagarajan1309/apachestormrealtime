package com.vinodh.storm.topology;

import com.vinodh.storm.topology.bolt.WriteToFileBolt;
import com.vinodh.storm.topology.spout.IntegerSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.TopologyBuilder;

public class ShuffleGroupingTopology {

    public static void main(String[] args) {

        // Builder
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("IntSpout", new IntegerSpout());
        builder.setBolt("FileOutSpout" , new WriteToFileBolt(),3).shuffleGrouping("IntSpout");

        StormTopology topology = builder.createTopology();

        // Config
        Config config = new Config();
        config.put("dirToWrite","/Users/vthiagarajan/Documents/WorkSpaces/VinodhWorkSpaces/yf-output/");

        // Cluster
        LocalCluster localCluster = new LocalCluster();

        try {
            localCluster.submitTopology("ShuffleGrouping",config,topology);
            Thread.sleep(4000);
        } catch (Exception e) {

        }finally {
            localCluster.shutdown();
        }


    }
}
