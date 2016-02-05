package com.gaea.tailsource.sender;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLException;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import com.gaea.tailsource.common.Objects;
import com.gaea.tailsource.common.StringUtils;

public class HttpClient {

	// 接口地址
	private String apiURL = "";
	private Log logger = LogFactory.getLog(this.getClass());
	private CloseableHttpClient httpClient = null;
	protected HttpPost method = null;
	private long startTime = 0L;
	private long endTime = 0L;
	
	private static final int MAX_RETRY_COUNT = 5;
	private static final int MAX_SOCKET_TIMEOUT = 5000;
	private static final int MAX_CONNECTION_TIMEOUT = 5000;
	/**
	 * 接口地址
	 * 
	 * @param url
	 */
	public HttpClient(String url) {

		if (url != null) {
			this.apiURL = url;
		}
		if (apiURL != null) {
			httpClient = HttpClients.custom().setMaxConnTotal(5)
					.setMaxConnPerRoute(5).setRetryHandler(new HttpRequestRetryHandler(){
						@Override
						public boolean retryRequest(IOException exception,
								int executionCount, HttpContext context) {
							Objects.startChain(exception).orInstanceof(InterruptedIOException.class, new Objects.ExpressHandler() {
								@Override
								public <E> boolean doHandler(E clz) {
									logger.error("HttpClient TimeOut.");
									return true;
								}
							}).orInstanceof(UnknownHostException.class, new Objects.ExpressHandler() {
								@Override
								public <E> boolean doHandler(E clz) {
									logger.error("Unknown host");	
									return true;
								}
							}).orInstanceof(ConnectTimeoutException.class, new Objects.ExpressHandler() {
								@Override
								public <E> boolean doHandler(E clz) {
									logger.error("Connecton timeout.");
									return true;
								}
							}).orInstanceof(SSLException.class, new Objects.ExpressHandler(executionCount) {
								@Override
								public <E> boolean doHandler(E clz) {
									logger.error("SSLException occurred!, try " + this.getData() + " times...");
									return true;
								}
							}).orInstanceof(HttpHostConnectException.class, new Objects.ExpressHandler() {
								@Override
								public <E> boolean doHandler(E clz) {
									logger.error("HostConnecton Refused.");
									return true;
								}
							});
							HttpClientContext clientContext = HttpClientContext.adapt(context);
					        HttpRequest request = clientContext.getRequest();
					        boolean idempotent = !(request instanceof HttpEntityEnclosingRequest);
					        if (idempotent) {
					            // Retry if the request is considered idempotent
					            return true;
					        }
							// finally 
							if (executionCount >= MAX_RETRY_COUNT){
								if (executionCount % MAX_RETRY_COUNT == 0){
									logger.error("Retry Request too times : " + executionCount, exception);
								}
								return true;
							}
							return true;
						}
					}).build();
			// new DefaultHttpClient();
			method = new HttpPost(apiURL);
			method.setConfig(RequestConfig.custom().setSocketTimeout(MAX_SOCKET_TIMEOUT)
					.setConnectTimeout(MAX_CONNECTION_TIMEOUT).build());
			method.addHeader("Connection", "Keep-Alive");
		}
	}

	/**
	 * 调用 API
	 * 
	 * @param parameters
	 * @return
	 */
	public String sendData(String parameters) {
		String body = null;
		checkNotNull(method, "method should not be null.");
		checkNotNull(parameters, "parameters is null.");
		checkArgument(!"".equals(parameters.trim()),
				"parameters value is empty.");
		try {
			List<NameValuePair> params = new ArrayList<NameValuePair>();
			// 建立一个NameValuePair数组，用于存储欲传送的参数
			params.add(new BasicNameValuePair("data", parameters));
			// 添加参数
			method.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
			// 设置编码
			HttpResponse response;
			int statusCode;
			while (true) {
				// try again
				response = httpClient.execute(method);
				statusCode = response.getStatusLine().getStatusCode();
				if(statusCode != HttpStatus.SC_OK){
					logger.error("Method failed:" + response.getStatusLine());
					EntityUtils.consumeQuietly(response.getEntity());
				}else{
					break;
				}
				try {
					TimeUnit.MILLISECONDS.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			// Read the response body
			body = EntityUtils.toString(response.getEntity());
			EntityUtils.consumeQuietly(response.getEntity());

		} catch (IOException e) {
			// 发生网络异常
			logger.error("exception occurred!\n"
					+ ExceptionUtils.getFullStackTrace(e));
			// 网络错误
		} 
		return body;
	}
	
	protected String assembleMessage(String ... messages){
		return StringUtils.join(System.lineSeparator(), messages);
	}
	
	public String sendData(String ... messages){
		String body = null;
		checkNotNull(messages, "messages is null.");
		String data = assembleMessage(messages);
		try {

			List<NameValuePair> params = new ArrayList<NameValuePair>();
			// 建立一个NameValuePair数组，用于存储欲传送的参数
			params.add(new BasicNameValuePair("data", data));
			// 添加参数
			method.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
			// 设置编码
			HttpResponse response;
			int statusCode;
			while (true) {
				// try again
				response = httpClient.execute(method);
				statusCode = response.getStatusLine().getStatusCode();
				if(statusCode != HttpStatus.SC_OK){
					logger.error("Method failed:" + response.getStatusLine());
					EntityUtils.consumeQuietly(response.getEntity());
				}else{
					break;
				}
				try {
					TimeUnit.MILLISECONDS.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			// Read the response body
			body = EntityUtils.toString(response.getEntity());
			EntityUtils.consumeQuietly(response.getEntity());
		} catch (IOException e) {
			// 发生网络异常
			logger.error("exception occurred!\n"
					+ ExceptionUtils.getFullStackTrace(e));
		} 
		return body;
	}


	/**
	 * @return the startTime
	 */
	public long getStartTime() {
		return startTime;
	}

	/**
	 * @return the endTime
	 */
	public long getEndTime() {
		return endTime;
	}
	
	public void close(){
		try {
			httpClient.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws InterruptedException {
		/*String s = java.security.AccessController.doPrivileged(
	            new sun.security.action.GetPropertyAction("line.separator"));*/
		//String s2 = System.lineSeparator();
		try{
			HttpClient hc = new HttpClient("http://localhost:8080/apimob/upload");
					//"http://123.59.60.17:8090/zeus/upload/default");
			long startTime = System.currentTimeMillis();
			for (int i=1;i<100000;i++)
			{
				hc.sendData("{\"networkType\":\"NONE\",\"appId\":\"20610232027b4c4c89d54d8cdc2e01712\",\"accountId\":\"2004269\",\"lost\":null,\"levelId\":\"44\",\"reason\":\"扫荡\",\"gaeaId\":\"1405925\",\"deviceResolution\":\"800*480\",\"mac\":\"aa:22:33:44:%5\",\"timeZone\":\"GMT+09:00\",\"endTime\":0,\"loginType\":\"login\",\"sdkVersion\":2,\"deviceType\":\"AndroidPhone\",\"age\":0,\"deviceOs\":\"4.2.2\",\"gender\":0,\"serverId\":\"2\",\"coinNum\":\"7086\",\"deviceId\":\"ZCXZXC\",\"platform\":\"android\",\"beginTime\":1442184581364,\"operator\":\"SKTelecom\",\"gain\":\"120\",\"dataType\":4,\"accountName\":\"카체리나\",\"duration\":0,\"session\":\"40eda6d1-8eec-4876-85d5-18c7bf206f2a\",\"appVersion\":\"2.5.0\",\"coinType\":\"金币\",\"deviceBrand\":\"samsung\",\"channel\":\"Nstore\",\"ip\":\"0:0:0:0:0:0:0:1\"}");
				/*System.out
						.println("i:"+i);*/
				//TimeUnit.MILLISECONDS.sleep(2000);
			}
			long endTime = System.currentTimeMillis();
			System.out.println(endTime - startTime + "ms");
			System.out.println("-------------------end-------------------------");
			/*
			File f = new File("F:\\work\\eclipse.rar");
			System.out.println("length:" + f.length());
			
			*/
		}
		catch(Exception e){
			System.out.println("异常");
			e.printStackTrace();
		}
	}
}