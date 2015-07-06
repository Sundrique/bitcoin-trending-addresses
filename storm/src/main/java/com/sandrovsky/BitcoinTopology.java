package com.sandrovsky;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.starter.bolt.IntermediateRankingsBolt;
import storm.starter.bolt.TotalRankingsBolt;

public class BitcoinTopology {

    private final static int TOP_N = 10;

    public static void main(String[] args) throws Exception
    {
        // create the topology
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("transaction", new TransactionSpout(), 10);

        builder.setBolt("address-and-value-bolt", new AddressAndValueBolt(), 3).shuffleGrouping("transaction");

        builder.setBolt("rolling-sum-bolt", new RollingSumBolt(30, 10), 1).fieldsGrouping("address-and-value-bolt", new Fields("address"));

        builder.setBolt("intermediate-ranker", new IntermediateRankingsBolt(TOP_N), 4).fieldsGrouping("rolling-sum-bolt", new Fields("address"));
        builder.setBolt("total-ranker", new TotalRankingsBolt(TOP_N)).globalGrouping("intermediate-ranker");

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
