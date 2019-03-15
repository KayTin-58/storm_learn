package trident.mapfunction;

import org.apache.storm.trident.operation.FlatMapFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;


/**
 * 切割
 */
public class Split implements FlatMapFunction {


  public Iterable<Values> execute(TridentTuple input) {
    List<Values> valuesList = new ArrayList<Values>();
    for (String word : input.getBinaryByField("sentence").toString().split(" ")) {
      valuesList.add(new Values(word));
    }
    return valuesList;
  }
}