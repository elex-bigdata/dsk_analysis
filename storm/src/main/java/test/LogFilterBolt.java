package test;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by wanghaixing on 15-11-10.
 */
public class LogFilterBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
//            byte[] binaryByField = tuple.getBinaryByField("bytes");
//            String value = new String(binaryByField);
            String value = tuple.getString(0);
            System.out.println(value);
            this.collector.ack(tuple);
        } catch (Exception e) {
            this.collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}