package storm_kafka.operators;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import storm_kafka.model.JsonObject;

/**
 * description
 *
 * @author zb 2019/03/06 21:28
 */
public class JsonPreFunction extends BaseFunction {

    private Fields fields;

    public JsonPreFunction(Fields fields) {
        this.fields = fields;
    }

    public JsonPreFunction() {
    }

    /**
     *
     * @param tridentTuple
     * @param tridentCollector
     */
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String json = tridentTuple.getString(0);
        //解析json,并过滤
        JsonObject jsonObject = resolveJson(json);
        Values values = new Values();
        values.add(jsonObject);
        tridentCollector.emit(values);
    }


    private JsonObject resolveJson(String json) {
        return null;
    }

}
