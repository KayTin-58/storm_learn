package trident.mapfunction;


import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import wordcount.Utils;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class SentenceSpout extends BaseRichSpout {


    private ConcurrentHashMap<UUID, Values> pending;
    private SpoutOutputCollector collector;
    private String[] sentences = {
        "my dog has fleas",
        "i like cold beverages",
        "the dog ate my homework",
        "don't have a cow man",
        "i don't think i like fleas"
    };
    private int index = 0;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("sentence"));
    }

    public void open(Map config, TopologyContext context,
            SpoutOutputCollector collector) {
        this.collector = collector;
        this.pending = new ConcurrentHashMap<UUID, Values>();
    }

    public void nextTuple() {


        if (index >= sentences.length) {
            while (true) {
                //do nothing
            }
        }
        Values values = new Values(sentences[index]);
        UUID msgId = UUID.randomUUID();
        this.pending.put(msgId, values);
        System.out.println("=====SentenceSpout发射tuple======="+values);
        this.collector.emit(values, msgId);
        index++;




        Utils.waitForMillis(1);
    }

    /**
     * 确认消息然后删除消息
     * @param msgId
     */
    public void ack(Object msgId) {
        System.out.println("====ack()======");
        this.pending.remove(msgId);
    }

    /**
     * 失败重发
     * @param msgId
     */
    public void fail(Object msgId) {
        System.out.println("===fail()====");
        this.collector.emit(this.pending.get(msgId), msgId);
    }
}
