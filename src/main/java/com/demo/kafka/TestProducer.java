package com.demo.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class TestProducer {
	
	
	public void testSend(){
		
		Properties props = new Properties();
		//broker列表
		props.put("metadata.broker.list", "h202:9092");
		//串行化
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		//
		props.put("request.required.acks", "1");

		//创建生产者配置对象
		ProducerConfig config = new ProducerConfig(props);

		//创建生产者
		Producer<String, String> producer = new Producer<String, String>(config);

		KeyedMessage<String, String> msg = new KeyedMessage<String, String>("test","2" ,"hello world tomas2");
		producer.send(msg);
		System.out.println("send over!");
		
	}
	
	    @Test
	    public void testConumser(){
	        //
	        Properties props = new Properties();
	        props.put("zookeeper.connect", "h202:2181");
	        props.put("group.id", "g1");
	        props.put("zookeeper.session.timeout.ms", "500");
	        props.put("zookeeper.sync.time.ms", "250");
	        props.put("auto.commit.interval.ms", "1000");
	        props.put("auto.offset.reset", "smallest");
	        //创建消费者配置对象
	        ConsumerConfig config = new ConsumerConfig(props);
	        //
	        Map<String, Integer> map = new HashMap<String, Integer>();
	        map.put("test", new Integer(1));
	        Map<String, List<KafkaStream<byte[], byte[]>>> msgs = Consumer.createJavaConsumerConnector(new ConsumerConfig(props)).createMessageStreams(map);
	        List<KafkaStream<byte[], byte[]>> msgList = msgs.get("test");
	        for(KafkaStream<byte[],byte[]> stream : msgList){
	            ConsumerIterator<byte[],byte[]> it = stream.iterator();
	            while(it.hasNext()){
	                byte[] message = it.next().message();
	                System.out.println(new String(message));
	            }
	        }
	        System.out.println("更新1");
		    System.out.println("在线更新");
		    System.out.println("dev");
	    }	
}
