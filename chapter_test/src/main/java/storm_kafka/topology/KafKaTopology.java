package storm_kafka.topology;


import kafka.api.OffsetRequest;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;
import storm_kafka.operators.JsonPreFunction;

import java.util.UUID;

/**
 * description
 *
 * @author zb 2019/03/04 20:15
 */
public class KafKaTopology {

    public static void main(String[] args) {

        //TopologyBuilder topologyBuilder = new TopologyBuilder();

        TridentTopology topology = new TridentTopology();
        BrokerHosts hosts = new ZkHosts("192.168.230.135:2181");
        SpoutConfig spoutConfig =
                new SpoutConfig(hosts, "first", "/kafka_storm" , UUID.randomUUID().toString());

        spoutConfig.startOffsetTime = OffsetRequest.EarliestTime();
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);


        Stream stream = topology.newStream("kafkaSpout",kafkaSpout);
        Fields fields =new Fields();
        stream.each(new Fields("bytes"),new JsonPreFunction(fields),fields);




        // topologyBuilder.setSpout("kafkaSpout",kafkaSpout);
        //
        //
        //
        // KafKaBolt kafKaBolt = new KafKaBolt();
        // topologyBuilder.setBolt("kafKaBolt",kafKaBolt).shuffleGrouping("kafkaSpout");

        Config config = new Config();
        config.setNumWorkers(2);

        LocalCluster cluster = new LocalCluster();

        //cluster.submitTopology(KafKaTopology.class.getSimpleName(), config, topologyBuilder.createTopology());



    }

}
