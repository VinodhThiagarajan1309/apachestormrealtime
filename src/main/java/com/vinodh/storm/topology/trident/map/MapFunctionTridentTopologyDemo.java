package com.vinodh.storm.topology.trident.map;

import com.vinodh.storm.topology.trident.SimpleTridentFunction;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;

public class MapFunctionTridentTopologyDemo {

    public static void main(String[] args) throws Exception {

        LocalDRPC drpc = new LocalDRPC();

        TridentTopology topology = new TridentTopology();
        topology.newDRPCStream("simple",drpc)
                .map(new MapFunctionDemo())
                .flatMap(new FlatMapFunctionDemo());

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("trident-topology", config, topology.build());

        for(String word :  new String[] {"THIS IS GOOD", "IS THAT GREAT", "WHERE IS THAT"}) {
            System.out.println(" Result for " + word + " : " + drpc.execute("simple",word));
        }

        cluster.shutdown();

    }
}
