package com.vinodh.storm.topology.drpc;

import com.vinodh.storm.topology.drpc.bolt.PlusTenBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;

public class DrpcTopology  {

    public static void main(String[] args) throws Exception {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("plusTen");
        builder.addBolt( new PlusTenBolt(),3);

        Config config = new Config();

        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("drpc-plusTen",config,builder.createLocalTopology(drpc));

        for(Integer number : new Integer[]{53,62,70}) {
            System.out.println( "Result for " + number + " : " + drpc.execute("plusTen", number.toString()));
        }

        Thread.sleep(10000);

        cluster.shutdown();
        drpc.shutdown();

    }
}
