package trident.mapfunction;

import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;

/**
 * description
 *
 * @author zb 2019/03/07 14:03
 */
public class Topology {

    public static void main(String[] args) {
        TridentTopology topology = new TridentTopology();
        SentenceSpout spout = new SentenceSpout();
        //
        Stream inputStream = topology.newStream("event", spout);

        inputStream.flatMap(new Split()).map(new UpperCase())
                .peek(new Consumer() {
                    public void accept(TridentTuple input) {
                        System.out.println(input.getString(0));
                    }
                })
                .groupBy(new Fields("sentence"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"));
    }

}
