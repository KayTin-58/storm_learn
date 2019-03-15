package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * description
 *
 * @author zb 2019/03/05 18:49
 */
public class NewProducer {
    public static void main(String[] args) {

      /*  Properties props = new Properties();
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "192.168.230.135:9092");
        // 等待所有副本节点的应答
        props.put("acks", "all");
        // 消息发送最大尝试次数
        props.put("retries", 2);
        // 一批消息处理大小
        props.put("batch.size", 1024);
        // 请求延时
        props.put("linger.ms", 100);
        // 发送缓存区内存大小
        props.put("buffer.memory", 1024);
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String,String>(props);
        for (int i = 0; i < 50; i++) {
            System.out.println("===="+i);//                        kafka_storm
            Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>(
                    "first",
                    Integer.toString(i),
                    "hello world-" + i), new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println(e.toString());
                    if (recordMetadata != null) {
                        System.err.println(recordMetadata.partition() + "---" + recordMetadata.offset());
                    }
                }
            });

        }

        producer.close();*/


        // ProducerConfig
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.230.135:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = null;
        try {
            //recordMetadata e
            producer = new KafkaProducer<String, String>(properties);
            // for (int i = 0; i < 100; i++) {
            //     String msg = "Message " + i;
            //     producer.send(new ProducerRecord<String, String>("first", msg),(recordMetadata, e) ->{
            //         System.out.println("recordMetadata:["+recordMetadata.toString());
            //     });
            //
            // }


            // int slowCount = 6;
            // int fastCount = 15;

            int i = 0;
            for( i= 0;i< 900000;i++) {
                String msg = "hello,wello,yyy,aaa,ccc,ddd,ccc,fff,"+i;
                producer.send(new ProducerRecord<String,String>("first", msg));
            }
            System.out.println("i:"+i);

            // for(int i= 0;i< fastCount;i++) {
            //     String msg = "fast msg ..........["+i+"]";
            //     producer.send(new ProducerRecord<String,String>("first", msg));
            //     Thread.sleep(1000);
            // }
            //
            // for(int i= 0;i< slowCount;i++) {
            //     String msg = "fast msg again ..........["+i+"]";
            //     producer.send(new ProducerRecord<String,String>("first", msg));
            //     Thread.sleep(5000);
            // }

        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            producer.close();
        }
    }
}

