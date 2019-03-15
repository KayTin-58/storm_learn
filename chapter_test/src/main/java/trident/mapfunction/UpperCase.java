package trident.mapfunction;

import org.apache.storm.trident.operation.MapFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class UpperCase implements MapFunction {

    /**
     * 大小写转换
     * @param input
     * @return
     */
 public Values execute(TridentTuple input) {
     //System.out.println("input:"+input.toString());
     return new Values(input.getString(0).toUpperCase());
 }
}