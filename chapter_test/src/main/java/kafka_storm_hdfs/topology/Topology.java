package kafka_storm_hdfs.topology;

import kafka_storm_hdfs.FileSizeAndTimedRotationPolicy;
import kafka_storm_hdfs.bolt.FortmatBolt;
import kafka_storm_hdfs.partition.DatePartition;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

/**
 * description
 *
 * @author zb 2019/03/14 11:25
 */
public class Topology {

    public static void main(String[] args) {

        KafkaSpout kafkaSpout = getKafkaSpout();

        // HDFS bolt
        // use "|" instead of "," for field delimiter
        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter(",");

        // rotate files when they reach 100
        FileSizeAndTimedRotationPolicy rotationPolicy = new
                FileSizeAndTimedRotationPolicy(
                        100, FileSizeAndTimedRotationPolicy.SizeUnit.MB,
                        2,FileSizeAndTimedRotationPolicy.TimeUnit.HOURS);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("/user/hive/warehouse/");

        SyncPolicy syncPolicy = new CountSyncPolicy(1000);
        HdfsBolt bolt = new HdfsBolt()
                .withFsUrl("hdfs://192.168.230.131:8020")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withPartitioner(new DatePartition("dt_date"))
                .withSyncPolicy(syncPolicy);


        /**
         * 设置3个worker
         */
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", kafkaSpout, 3)
                .setNumTasks(3);
        builder.setBolt("fortmatBolt",new FortmatBolt(),3)
                .setNumTasks(3)
                .shuffleGrouping("spout");
        builder.setBolt("hdfs", bolt,3)
                .setNumTasks(3)
                .shuffleGrouping("fortmatBolt");

        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(3);

        if(args != null && args.length >0) {
            try {
                StormSubmitter.submitTopology("",config,builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        }else {
            config.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("Wordcount-Topology", config, builder.createTopology());
            try {
                Thread.sleep(500000);
            } catch (InterruptedException e) {
                e.printStackTrace();
                cluster.shutdown();
            }
            cluster.shutdown();
        }


     }







    static KafkaSpout getKafkaSpout() {
        String zkHost = "192.168.230.131:2181,192.168.230.132:2181,192.168.230.133:2181,";
        String topic = "kafkaTopic";
        String groupId = "id";
        ZkHosts zkHosts = new ZkHosts(zkHost);//kafaka所在的zookeeper
        SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic, "/storm", groupId);  // create /order /id
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        return kafkaSpout;
    }




}
