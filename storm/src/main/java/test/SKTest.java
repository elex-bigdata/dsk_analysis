package test;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.UUID;

/**
 * Created by wanghaixing on 15-11-10.
 */
public class SKTest {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        TopologyBuilder builder = new TopologyBuilder();
        BrokerHosts hosts = new ZkHosts("datanode1:2181,datanode2:2181,datanode4:2181");
        String topic = "topic1";
        String zkRoot = "/kafkaspout";
        String id = UUID.randomUUID().toString();
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, id);
        String SPOUT_ID = KafkaSpout.class.getSimpleName();
        String BOLT_ID = LogFilterBolt.class.getSimpleName();

        builder.setSpout(SPOUT_ID, new KafkaSpout((spoutConfig)));
        builder.setBolt(BOLT_ID, new LogFilterBolt()).shuffleGrouping(SPOUT_ID);

//        LocalCluster localCluster = new LocalCluster();
//        localCluster.submitTopology(SKTest.class.getSimpleName(), new Config(), builder.createTopology());

        Config stromConfig = new Config();
        StormSubmitter.submitTopology("testTopology", stromConfig, builder.createTopology());




    }
}