package local_ksh.bolt;

import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;

public class CounterBolt extends BaseBasicBolt {
	private static final long serialVersionUID = -5508421065181891596L;

	private static Logger logger = Logger.getLogger(CounterBolt.class);

	private static long counter = 0;


	public void execute(Tuple tuple, BasicOutputCollector collector) {
		List<Object> data = tuple.getValues();

		String id = (String) data.get(0);
		String memberid = (String) data.get(1);
		String totalprice = (String) data.get(2);
		String preprice = (String) data.get(3);
		String sendpay = (String) data.get(4);
		String createdate = (String) data.get(5);
		collector.emit(new Values(id,memberid,totalprice,preprice,sendpay,createdate));
		logger.info("+++++++++++++++++++++++++++++++++Valid+++++++++++++++++++++++++++++++++");
		logger.info("msg = "+data+" ----@-@-@-@-@--------counter = "+(counter++));
		logger.info("+++++++++++++++++++++++++++++++++Valid+++++++++++++++++++++++++++++++++");
	}


	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id","memberid","totalprice","preprice","sendpay","createdate"));
	}
}