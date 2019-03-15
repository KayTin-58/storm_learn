package kafka_storm_hdfs.sendmsg; /**
 * Created by whoami on 2016/11/24.
 */

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * 制造数据
 */
public class SendMessageKafka {

    public static void main(String[] args) {
        Properties props = new Properties();
        String zkHost = "192.168.230.131:2181,192.168.230.132:2181,192.168.230.133:2181,";
        String kafkaHost = "192.168.230.131:9092,192.168.230.132:9092,192.168.230.133:9092,";
        String kafkaTopic = "test";
        props.put("zookeeper.connect",
                zkHost);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("producer.type", "async");
        props.put("compression.codec", "1");
        props.put(
                "metadata.broker.list",
                kafkaHost);

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Random r = new Random();
        //模拟发射10000000的数据到kafka
        for (int i = 0; i < 10000000; i++) {
            int id = r.nextInt(10000000);
            int memberid = r.nextInt(100000);
            int totalprice = r.nextInt(1000) + 100;
            int preferential = r.nextInt(100);
            int sendpay = r.nextInt(3);

            StringBuffer data = new StringBuffer();
            data.append(String.valueOf(id)).append(",")
                    .append(String.valueOf(memberid)).append(",")
                    .append(String.valueOf(totalprice)).append(",")
                    .append(String.valueOf(preferential)).append(",")
                    .append(String.valueOf(sendpay)).append(",")
                    .append(df.format(new Date()));
            System.out.println(data.toString());
            producer.send(new KeyedMessage<String, String>("kafkaTopic", data
                    .toString()));
        }
        producer.close();
        System.out.println("send over ------------------");
    }

}