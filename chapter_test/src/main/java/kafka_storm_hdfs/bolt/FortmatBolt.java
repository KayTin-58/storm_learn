package kafka_storm_hdfs.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * description
 *
 * @author zb 2019/03/14 11:16
 */
public class FortmatBolt extends BaseRichBolt {

    private OutputCollector outputCollector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
              this.outputCollector = outputCollector;
    }

    public void execute(Tuple tuple) {
         String data = tuple.getString(0);


        if(data!=null && data.length()>0) {
            String[] values = data.split("\t");
            if(values.length==6) {
                String id = values[0];
                String memberid = values[1];
                String totalprice = values[2];
                String preprice = values[3];
                String sendpay = values[4];
                String createdate = values[5];

                outputCollector.emit(new Values(id,memberid,totalprice,preprice,sendpay,createdate));
            }

        }



    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id","memberid","totalprice","preprice","sendpay","createdate"));
    }
}
