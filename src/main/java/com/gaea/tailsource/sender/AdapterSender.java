package com.gaea.tailsource.sender;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

public class AdapterSender {

	private HttpClient httpClient;
	private KafkaClient kafkaClient;

	final static String KAFKA_CLIENT_TYPE = "kafka";
	final static String HTTP_CLIENT_TYPE = "http";
	final static String SDK_HTTP_CLIENT_TYPE = "sdkhttp";
	private int clientIndex = -1;

	public AdapterSender(int clientIndex, Object parameter) throws IOException,
			InterruptedException {
		if ((this.clientIndex = clientIndex) == 1) {
			httpClient = new HttpClient(String.valueOf(parameter));
		} else if ((this.clientIndex = clientIndex) == 0) {
			kafkaClient = KafkaClient.getInstance();
		} else if ((this.clientIndex = clientIndex) == 2) {
			httpClient = new SDKHttpClient(String.valueOf(parameter));
		}
	}

	public AdapterSender(String clientType, Object parameter)
			throws IOException, InterruptedException {
		checkNotNull(clientType);
		if (HTTP_CLIENT_TYPE.equalsIgnoreCase(clientType)) {
			clientIndex = 1;
			httpClient = new HttpClient(String.valueOf(parameter));
		} else if (KAFKA_CLIENT_TYPE.equalsIgnoreCase(clientType)) {
			clientIndex = 0;
			kafkaClient = KafkaClient.getInstance();
		} else if (SDK_HTTP_CLIENT_TYPE.equalsIgnoreCase(clientType)) {
			clientIndex = 2;
			httpClient = new SDKHttpClient(String.valueOf(parameter));
		}
	}

	public void send(String[] messages) throws InterruptedException,
			IOException {
		if (messages == null || messages.length == 0) {
			return;
		}
		if (clientIndex == 0) {
			kafkaClient.sendData(messages);
		} else {
			httpClient.sendData(messages);
		}
	}

	public void send(String message) throws InterruptedException {
		if (message == null) {
			return;
		}
		if (clientIndex == 0) {
			kafkaClient.sendData(message);
		} else {
			httpClient.sendData(message);
		}
	}

	public void close() {
		if (clientIndex == 0) {
			kafkaClient.close();
		} else {
			httpClient.close();
		}
	}

}
