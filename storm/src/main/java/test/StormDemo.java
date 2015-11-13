package test;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;
import storm.kafka.bolt.KafkaBolt;

import java.util.Properties;
import java.util.UUID;

/**
 * @author yanbit
 * @date Nov 10, 2015 11:04:44 AM
 * @todo TODO
 */
public class StormDemo {
  public static void main(String[] args)
      throws AlreadyAliveException, InvalidTopologyException,
      AuthorizationException {
    TopologyBuilder builder = new TopologyBuilder();

    // config kafka spout
    String topicName = "demo";
    BrokerHosts hosts =
        new ZkHosts("10.1.3.56:2181,10.1.3.57:2181,10.1.3.59:2181");
    SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName,
        UUID.randomUUID().toString());
    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

    // set kafka spout
    builder.setSpout("kafka_spout", kafkaSpout, 4);

    // set bolt
    builder.setBolt("check", new CheckBolt(), 8).shuffleGrouping("kafka_spout");
    // builder.setBolt("filter", new FilterShellBolt(),
    // 8).shuffleGrouping("kafka_spout");

    // // set kafka bolt
    // KafkaBolt kafka_bolt = new KafkaBolt()
    // .withTopicSelector(new DefaultTopicSelector(topicName + "_ERROR"))
    // .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
    // builder.setBolt("kafka_bolt", kafka_bolt, 2).shuffleGrouping("check");

    Config conf = new Config();

    //out
    // set producer properties.
    Properties props = new Properties();
    props.put("metadata.broker.list",
        "10.1.3.55:9092,10.1.3.56:9092,10.1.3.59:9092");
    props.put("request.required.acks", "1");
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    conf.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);

    conf.setNumWorkers(2);
    StormSubmitter.submitTopologyWithProgressBar("logfilter", conf,
        builder.createTopology());

    // 测试环境采用 local mode 模式
    //LocalCluster cluster = new LocalCluster();
    //cluster.submitTopology("demo", conf, builder.createTopology());
    // cluster.killTopology("demo");
    // cluster.shutdown();
  }
}
