package com.vinodh.storm.topology.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;

public class TridentTopologyDemo {

    public static void main(String[] args) throws Exception {

        LocalDRPC drpc = new LocalDRPC();
        TridentTopology topology = new TridentTopology();
        topology.newDRPCStream("simple",drpc)
                .each(new Fields("args"),
                        new SimpleTridentFunction(),
                        new Fields("processed_word"));

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("trident-topology", config, topology.build());

        for(String word :  new String[] {"word 1", "word 2", "word 3"}) {
            System.out.println(" Result for " + word + " : " + drpc.execute("simple",word));
        }

        cluster.shutdown();

    }
}
