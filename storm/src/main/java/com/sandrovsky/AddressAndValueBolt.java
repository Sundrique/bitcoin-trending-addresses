package com.sandrovsky;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Map;

public class AddressAndValueBolt extends BaseRichBolt {

    private static final Logger LOG = Logger.getLogger(AddressAndValueBolt.class);

    OutputCollector collector;

    @Override
    public void prepare(
            Map map,
            TopologyContext topologyContext,
            OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String transaction = tuple.getString(0);

        try {
            JSONObject message = new JSONObject(transaction);

            JSONArray outs = message
                    .getJSONObject("x")
                    .getJSONArray("out");

            for (int i = 0; i < outs.length(); i++) {
                JSONObject out = outs.getJSONObject(i);
                String address = out.getString("addr");
                Long value = out.getLong("value");

                collector.emit(new Values(address, value));
            }

        } catch (JSONException e) {
            LOG.error(e.getMessage());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("address", "value"));
    }
}
