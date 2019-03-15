package local_ksh.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Andrew on 2015-5-18.
 */
public class CheckOrderBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 8602883858475659429L;

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");//设置日期格式
		String nowData = df.format(new Date()); // new Date()为获取当前系统时间,检测是否为最新数据

		String data = tuple.getString(0);
		//订单号		用户id	     原金额	                      优惠价	          标示字段		下单时间
		//id		memberid  	totalprice					preprice		sendpay		createdate
		if(data!=null && data.length()>0) {
			String[] values = data.split("\t");
			if(values.length==6) {
				String id = values[0];
				String memberid = values[1];
				String totalprice = values[2];
				String preprice = values[3];
				String sendpay = values[4];
				String createdate = values[5];
				collector.emit(new Values(id,memberid,totalprice,preprice,sendpay,createdate));

				}
			}
		}


	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id","memberid","totalprice","preprice","sendpay","createdate"));
	}

}