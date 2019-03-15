package local_ksh.kafka_send;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class SendMessageKafka {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("zookeeper.connect",
                "192.168.230.135:2181");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("producer.type", "async");
        props.put("compression.codec", "1");
        props.put(
                "metadata.broker.list",
                "192.168.230.135:9092");

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Random r = new Random();
        for (int i = 0; i < 1000000; i++) {
            int id = r.nextInt(10000000);
            int memberid = r.nextInt(100000);
            int totalprice = r.nextInt(1000) + 100;
            int preferential = r.nextInt(100);
            int sendpay = r.nextInt(3);

            StringBuffer data = new StringBuffer();
            data.append(String.valueOf(id)).append("\t")
                    .append(String.valueOf(memberid)).append("\t")
                    .append(String.valueOf(totalprice)).append("\t")
                    .append(String.valueOf(preferential)).append("\t")
                    .append(String.valueOf(sendpay)).append("\t")
                    .append(df.format(new Date()));
            System.out.println(data.toString());
            producer.send(new KeyedMessage<String, String>("kshTopic", data
                    .toString()));
        }
        producer.close();
        System.out.println("send over ------------------");
    }

}