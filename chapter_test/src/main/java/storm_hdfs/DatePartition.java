package storm_hdfs;


import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.storm.hdfs.common.Partitioner;
import org.apache.storm.tuple.Tuple;

import java.util.Date;

/**
 * description
 *
 * @author zb 2019/03/14 11:00
 */
public class DatePartition implements Partitioner {

    //Hive表分区字段值
    private String hive_partition_filed;

    public DatePartition(String hive_partition_filed) {
        this.hive_partition_filed = hive_partition_filed;
    }

    /**
     * Return a relative path that the tuple should be written to. For example, if an HdfsBolt were configured to write
     * to /common/output and a partitioner returned "/foo" then the bolt should open a file in "/common/output/foo"
     *
     * A best practice is to use Path.SEPARATOR instead of a literal "/"
     *
     * @param tuple The tuple for which the relative path is being calculated.
     * @return
     */
    public String getPartitionPath(Tuple tuple) {
        String format = DateFormatUtils.format(new Date(), "yyyy-MM-dd");
        return this.hive_partition_filed+"="+format;
    }
}
