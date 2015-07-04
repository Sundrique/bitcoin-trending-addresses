package com.sandrovsky;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class BitcoinTopology {

    public static void main(String[] args) throws Exception
    {
        // create the topology
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("transaction", new TransactionSpout(), 10);

        builder.setBolt("exclaim", new AddressAndValueBolt(), 3).shuffleGrouping("transaction");

        Config conf = new Config();

        conf.setDebug(true);

        if (args != null && args.length > 0) {

            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {

            LocalCluster cluster = new LocalCluster();

            cluster.submitTopology("bitcoin", conf, builder.createTopology());
        }
    }
}
