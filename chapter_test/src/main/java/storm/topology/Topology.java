package storm.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import storm.bolt.MyBolt;
import storm.spout.MySpout;

/**
 * description
 *
 * @author zb 2019/03/04 11:35
 */
public class Topology {

    public static void main(String[] args) throws Exception {

        //创建
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        MySpout mySpout = new MySpout();
        MyBolt myBolt = new MyBolt();
        topologyBuilder.setSpout("mySpout",mySpout);
        topologyBuilder.setBolt("myBolt",myBolt).shuffleGrouping("mySpout");
        Config config = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Topology.class",config,topologyBuilder.createTopology());



        // SentenceSpout spout = new SentenceSpout();
        // SplitSentenceBolt splitBolt = new SplitSentenceBolt();
        // WordCountBolt countBolt = new WordCountBolt();
        // ReportBolt reportBolt = new ReportBolt();
        //
        //
        // TopologyBuilder builder = new TopologyBuilder();
        //
        // builder.setSpout(SENTENCE_SPOUT_ID, spout);
        // // SentenceSpout --> SplitSentenceBolt
        // builder.setBolt(SPLIT_BOLT_ID, splitBolt)
        //         .shuffleGrouping(SENTENCE_SPOUT_ID);
        // // SplitSentenceBolt --> WordCountBolt
        // builder.setBolt(COUNT_BOLT_ID, countBolt)
        //         .fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
        // // WordCountBolt --> ReportBolt
        // builder.setBolt(REPORT_BOLT_ID, reportBolt)
        //         .globalGrouping(COUNT_BOLT_ID);
        //
        // Config config = new Config();
        //
        // LocalCluster cluster = new LocalCluster();
        //
        // cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        // waitForSeconds(10);
        // cluster.killTopology(TOPOLOGY_NAME);
        // cluster.shutdown();



    }
}
