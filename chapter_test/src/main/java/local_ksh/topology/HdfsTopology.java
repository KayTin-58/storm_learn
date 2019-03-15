package local_ksh.topology;

import local_ksh.bolt.CheckOrderBolt;
import local_ksh.bolt.CounterBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

/**
 *
 * https://github.com/apache/storm/tree/master/external/storm-hdfs
 */
public class HdfsTopology {
    public static void main(String[] args) {
        try {
            String zkhost = "192.168.230.135:2181";
            String topic = "first";
            String groupId = "id";
            int spoutNum = 3;
            int boltNum = 1;
            ZkHosts zkHosts = new ZkHosts(zkhost);//kafaka所在的zookeeper
            SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic, "/order", groupId);  // create /order /id
            spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
            KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

            // HDFS bolt
            // use "|" instead of "," for field delimiter
            RecordFormat format = new DelimitedRecordFormat()
                    .withFieldDelimiter("|");

            // sync the filesystem after every 1k tuples
            SyncPolicy syncPolicy = new CountSyncPolicy(1000);

            // rotate files when they reach 5MB
            FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);
            // FileRotationPolicy rotationPolicy = new TimedRotationPolicy(1.0f, TimedRotationPolicy.TimeUnit.MINUTES);

            FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                    .withPath("/user/hive/warehouse").withPrefix("order_").withExtension(".log");

            HdfsBolt hdfsBolt = new HdfsBolt()
                    .withFsUrl("hdfs://192.168.230.135:8020")
                    .withFileNameFormat(fileNameFormat)
                    .withRecordFormat(format)
                    .withRotationPolicy(rotationPolicy)
                    .withSyncPolicy(syncPolicy);

            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("spout", kafkaSpout, spoutNum);
            builder.setBolt("check", new CheckOrderBolt(), boltNum).shuffleGrouping("spout");
            builder.setBolt("counter", new CounterBolt(), boltNum).shuffleGrouping("check");
            builder.setBolt("hdfs", hdfsBolt, boltNum).shuffleGrouping("counter");

            Config config = new Config();
            config.setDebug(true);

            if (args != null && args.length > 0) {
                config.setNumWorkers(2);
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } else {
                config.setMaxTaskParallelism(2);

                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("Wordcount-Topology", config, builder.createTopology());

                Thread.sleep(500000);

                cluster.shutdown();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}