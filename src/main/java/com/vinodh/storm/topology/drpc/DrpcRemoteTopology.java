package com.vinodh.storm.topology.drpc;

import com.vinodh.storm.topology.drpc.bolt.PlusTenBolt;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;

import java.util.ArrayList;
import java.util.List;

public class DrpcRemoteTopology {

    public static void main(String[] args)  throws Exception {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("remote-drpc-plusTen");
        builder.addBolt( new PlusTenBolt(),3);

        Config config = new Config();
        List<String> drpcServers = new ArrayList<>();
        drpcServers.add("localhost");

        config.put(Config.DRPC_SERVERS, drpcServers);
        config.put(Config.DRPC_PORT, 3772);

        StormSubmitter.submitTopology("remote-drpc-plusTen",config,builder.createRemoteTopology());

    }

}
