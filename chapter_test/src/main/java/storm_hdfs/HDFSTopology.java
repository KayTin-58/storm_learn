package storm_hdfs;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.NoRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.topology.TopologyBuilder;

/**
 * description
 *
 * @author zb 2019/03/12 11:06
 */
public class HDFSTopology {

    static final String SENTENCE_SPOUT_ID = "sentence-spout";
    static final String BOLT_ID = "my-bolt";
    static final String TOPOLOGY_NAME = "test-topology";


    public static void main(String[] args) {
        Config config = new Config();
        config.setNumWorkers(1);

        SentenceSpout spout = new SentenceSpout();
        FileSizeAndTimedRotationPolicy rotationPolicy = new
                FileSizeAndTimedRotationPolicy(
                128, FileSizeAndTimedRotationPolicy.SizeUnit.MB,
                2,FileSizeAndTimedRotationPolicy.TimeUnit.HOURS);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("/user/hive/warehouse/");

        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter("|");
        SyncPolicy syncPolicy = new CountSyncPolicy(100);




        HdfsBolt bolt = new HdfsBolt()
                .withFsUrl("hdfs://192.168.230.135:8020")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(new NoRotationPolicy())
                .withPartitioner(new DatePartition("dt_date"))
                .withSyncPolicy(syncPolicy);


        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SENTENCE_SPOUT_ID, spout, 2);
        // SentenceSpout --> MyBolt
        builder.setBolt(BOLT_ID, bolt, 4)
                .shuffleGrouping(SENTENCE_SPOUT_ID);
        config.setNumWorkers(2);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("cdc", config, builder.createTopology());

    }

}
