package storm.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * description
 *
 * @author zb 2019/03/04 11:37
 */
public class MySpout extends BaseRichSpout {


    private SpoutOutputCollector collector;
    private String[] sentences = {
            "my dog has fleas",
            "i like cold beverages",
            "the dog ate my homework",
            "don't have a cow man",
            "i don't think i like fleas"
    };

    /**
     * 结束标记
     */
    boolean falg = true;
    int index = 0;

    public void open(Map map, TopologyContext topologyContext,
                     SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    /**
     * 核心方法 发送数据
     *
     */
    public void nextTuple() {
        try {
            this.collector.emit(new Values(sentences[index]));
            index++;
            if (index >= sentences.length) {
                index = 0;
            }
            Thread.sleep(10 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }


    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
    }


    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
    }
}
