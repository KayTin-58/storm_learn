package storm_kafka.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * description
 *
 * @author zb 2019/03/04 20:11
 */
public class KafKaBolt extends BaseRichBolt {

    private OutputCollector outputCollector;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    public void execute(Tuple tuple) {
        String value = new String(tuple.getBinaryByField("bytes"));
        System.out.println("===storm_kafka.bolt.KafKaBolt.execute===[tiple:"+tuple.toString()+"]==" +
                "===[value:"+value);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
