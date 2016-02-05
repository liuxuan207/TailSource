package com.gaea.tailsource.sender;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gaea.tailsource.PropertiesLoader;
import com.gaea.tailsource.common.Objects;

/**
 * @author LIUXUAN 用于往kafka写数据， invoked by KafkaWriter
 */
public class KafkaClient {

	protected static Logger LOG = LoggerFactory.getLogger(KafkaClient.class);
	public static Random rnd = new Random();
	public static ProducerConfig config;
	public Producer<String, String> producer;
	public static String TOPIC = "zeus";// System.getProperty("config");
	public static int PARTITIONS = 8;
	public static String BROKERS = "localhost:9092";
	private int retryInterval = 100;
	private int maxRetryInterval = 5000;

	public static void systemInit() throws IOException {
		Properties sysPro = PropertiesLoader.loadProperties(
				System.getProperty("config"), "utf-8");
		TOPIC = sysPro.getProperty("topic", "test");
		PARTITIONS = Integer.parseInt(sysPro.getProperty("partitions", "8"));
		BROKERS = sysPro.getProperty("brokers", "localhost:9092");
		LOG.info("Load " + System.getProperty("config") + " successfully.");
		LOG.info("OVERWRITE TOPIC TO : " + TOPIC);
	}

	/**
	 * init producer configuration of kafkaClient
	 */
	static {
		try {
			systemInit();
		} catch (IOException e) {
			System.exit(1);
		}
		Properties props = new Properties();
		props.put("metadata.broker.list", BROKERS);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "com.gaea.tailsource.sender.CustomPartitioner");
		// 可选配置，如果不配置，则使用默认的partitioner
		// 触发acknowledgement机制，否则是fire and forget，可能会引起数据丢失
		// 值为0,1,-1,可以参考
		// http://kafka.apache.org/08/configuration.html
		props.put("request.required.acks", "1");
		props.put("request.timeout.ms", "3000");
		// props.put("message.send.max.retries", "2");
		// 同步还是异步发送消息，默认“sync”表同步，"async"表异步。异步可以提高发送吞吐量,
		// 也意味着消息将会在本地buffer中,并适时批量发送，但是也可能导致丢失未发送过去的消息
		props.put("producer.type", "async");
		// # 在async模式下,当message被缓存的时间超过此值后,将会批量发送给broker,默认为5000ms
		// # 此值和batch.num.messages协同工作.
		props.put("queue.buffering.max.ms", "1000");
		/*
		 * # 在async模式下,producer端允许buffer的最大消息量 #
		 * 无论如何,producer都无法尽快的将消息发送给broker,从而导致消息在producer端大量沉积 #
		 * 此时,如果消息的条数达到阀值,将会导致producer端阻塞或者消息被抛弃，默认为10000
		 */
		props.put("queue.buffering.max.messages", "30000");
		/* # 如果是异步，指定每次批量发送数据量，默认为200 */
		props.put("batch.num.messages", "500");
		/*
		 * # 当消息在producer端沉积的条数达到"queue.buffering.max.meesages"后 #
		 * 阻塞一定时间后,队列仍然没有enqueue(producer仍然没有发送出任何消息) #
		 * 此时producer可以继续阻塞或者将消息抛弃,此timeout值用于控制"阻塞"的时间 # -1: 无阻塞超时限制,消息不会被抛弃 #
		 * 0:立即清空队列,消息被抛弃
		 */
		props.put("queue.enqueue.timeout.ms", "-1");

		config = new ProducerConfig(props);
	}

	/**
	 * get an instance of kafkaClient
	 * 
	 * @return kafkaClient instance
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static KafkaClient getInstance() throws IOException,
			InterruptedException {
		return new KafkaClient();
	}

	/**
	 * @throws InterruptedException
	 * 
	 */
	@SuppressWarnings("unchecked")
	private KafkaClient() throws InterruptedException {
		// producer = new Producer<String, String>(config);
		Objects o = Objects.startChain(null).withRetryHandler(retryInterval,
				maxRetryInterval, 3, true, new Objects.ExpressHandler() {
					@Override
					public <E> boolean doHandler(E clz) {
						setDate(new Producer<String, String>(config));
						return true;
					}
				});
		if (o.result()) {
			System.out.println("Instance KafkaClient successfully.");
			producer = (Producer<String, String>) o.getResultData();
			//System.out.println(producer);
		}
	}

	/**
	 * one question not to be fixed : i can not capture the exception inside
	 * producer.
	 * 
	 * @param line
	 * @throws InterruptedException
	 */
	public void sendData(String line) throws InterruptedException {
		// 如果topic不存在，则会自动创建，默认replication-factor为1，partitions为0-2
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(
				TOPIC, String.valueOf(Integer.valueOf(rnd.nextInt(PARTITIONS))), line);
		//producer.send(data);
		Objects.startChain(null).withRetryHandler(retryInterval,
				maxRetryInterval, 3, true, new Objects.ExpressHandler(data) {
					@SuppressWarnings("unchecked")
					@Override
					public <E> boolean doHandler(E clz) {
						producer.send((KeyedMessage<String, String>) getData());
						return true;
					}
		});
	}

	public void sendData(String... lines) throws InterruptedException {
		List<KeyedMessage<String, String>> list = new ArrayList<KeyedMessage<String, String>>();
		String partitionKey = String.valueOf(rnd.nextInt(PARTITIONS));
		for (String line : lines) {
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(
					TOPIC, partitionKey, line);
			list.add(data);
		}
		int processCountWithException = 0;
		int _retryInterval = retryInterval;
		while (true) {
			try {
				if (processCountWithException >= 2) {
					try {
						LOG.error("kafkaClient send data with exception and has retried more than 3 times, try to get new kafkaClient instance.");
						close();
						producer = null; // help gc;
						producer = new Producer<String, String>(config);
						processCountWithException = 0;
					} catch (Exception e) {
						LOG.error("renew kafkaClient instance faild.", e);
						continue;
					}
				}
				producer.send(list);
				// send successfully and break;
				break;
			} catch (Exception ex) {
				LOG.warn("The channel is full or unexpected failure. "
						+ "The source will try again after " + _retryInterval
						+ " ms");
				TimeUnit.MILLISECONDS.sleep(_retryInterval);
				_retryInterval = _retryInterval << 1;
				_retryInterval = Math.min(_retryInterval, maxRetryInterval);
				processCountWithException++;
				continue;
			}
		}
	}

	public void sendData(List<String> lines) throws InterruptedException {
		sendData(lines.toArray(new String[0]));
	}

	public void close() {
		producer.close();
	}

	public static void main(String[] args) {
		try {
			KafkaClient client = KafkaClient.getInstance();
			client.sendData(new String[]{"test"});
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("errdddddddddddddddd");
		}
	}

}
