package com.sandrovsky;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TransactionSpout extends BaseRichSpout {
    SpoutOutputCollector collector;
    LinkedBlockingQueue<String> queue = null;
    BlockchainInfoClient client;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;
        queue = new LinkedBlockingQueue<String>(1000);

        client = new BlockchainInfoClient();
        client.addMessageHandler(new BlockchainInfoClient.MessageHandler() {
            @Override
            public void handleMessage(String message) {
                queue.offer(message);
            }
        });
    }

    @Override
    public void nextTuple()
    {
        String ret = queue.poll();

        if (ret==null)
        {
            Utils.sleep(50);
            return;
        }

        collector.emit(new Values(ret));
    }

    @Override
    public void close()
    {
        // shutdown the stream - when we are going to exit
    }

    @Override
    public Map<String, Object> getComponentConfiguration()
    {
        Config ret = new Config();

        ret.setMaxTaskParallelism(1);

        return ret;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("transaction"));
    }

}

