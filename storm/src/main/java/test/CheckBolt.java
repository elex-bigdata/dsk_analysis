package test;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * @author yanbit
 * @date Nov 10, 2015 11:39:15 AM
 * @todo TODO
 */
public class CheckBolt implements IBasicBolt {

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("fields"));
  }

  public void prepare(Map stormConf, TopologyContext context) {

  }

  public void execute(Tuple input, BasicOutputCollector collector) {
    try {
      String fields = input.getString(0);
      if (fields.split(",").length != 2) {
        System.out.println("==============WARN:" + fields);
      }
      System.out.println("=============fields:" + fields);
      collector.emit(new Values(fields));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void cleanup() {
  }

  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

}
