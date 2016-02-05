package com.gaea.tailsource.sender;

import com.gaea.tailsource.common.StringUtils;

public final class SDKHttpClient extends HttpClient{

	public SDKHttpClient(String url) {
		super(url);
	}

	
	@Override
	protected String assembleMessage(String... messages) {
		
		return "[" + StringUtils.join(",", messages) + "]";
	}


	public static void main(String[] args) throws InterruptedException {
		/*
		 * String s = java.security.AccessController.doPrivileged( new
		 * sun.security.action.GetPropertyAction("line.separator"));
		 */
		// String s2 = System.lineSeparator();
		try {
			SDKHttpClient hc = new SDKHttpClient(
					"http://localhost:8080/apimob/upload");
			// "http://123.59.60.17:8090/zeus/upload/default");
			long startTime = System.currentTimeMillis();
			for (int i = 1; i < 10; i++) {
				hc.sendData(
						"{\"beginTime\":1445526251949,\"levelId\":\"71\",\"dataType\":1,\"deviceId\":\"C9CAAE29-A698-4DC8-9BB8-73C7C8FFD2C1\",\"gaeaId\":\"2465169\",\"accountName\":\"money730324\",\"channel\":\"AppStore\",\"appVersion\":\"1.1.0\",\"action\":\"2\",\"endTime\":1445526932114,\"serverId\":\"1008\",\"session\":\"0b9547420afe33a64d0ae48a88878c45\",\"loginType\":\"login\",\"platform\":\"iOS\",\"mac\":\"020000000000\",\"gender\":null,\"idfv\":\"A8CDA613-EC7D-4725-B055-B478570E92DE\",\"appId\":\"a1801b7d59207a3629408cbf96a2d161\",\"age\":0,\"accountId\":\"24651100869\",\"idfa\":\"C9CAAE29-A698-4DC8-9BB8-73C7C8FFD2C1\",\"ip\":\"123.241.174.107\"}",
						"{\"networkType\":\"NONE\",\"appId\":\"20610232027b4c4c89d54d8cdc2e01712\",\"accountId\":\"2004269\",\"lost\":null,\"levelId\":\"44\",\"reason\":\"扫荡\",\"gaeaId\":\"1405925\",\"deviceResolution\":\"800*480\",\"mac\":\"aa:22:33:44:%5\",\"timeZone\":\"GMT+09:00\",\"endTime\":0,\"loginType\":\"login\",\"sdkVersion\":2,\"deviceType\":\"AndroidPhone\",\"age\":0,\"deviceOs\":\"4.2.2\",\"gender\":0,\"serverId\":\"2\",\"coinNum\":\"7086\",\"deviceId\":\"ZCXZXC\",\"platform\":\"android\",\"beginTime\":1442184581364,\"operator\":\"SKTelecom\",\"gain\":\"120\",\"dataType\":2,\"accountName\":\"카체리나\",\"duration\":0,\"session\":\"40eda6d1-8eec-4876-85d5-18c7bf206f2a\",\"appVersion\":\"2.5.0\",\"coinType\":\"金币\",\"deviceBrand\":\"samsung\",\"channel\":\"Nstore\",\"ip\":\"0:0:0:0:0:0:0:1\"}");
				/*
				 * System.out .println("i:"+i);
				 */
				// TimeUnit.MILLISECONDS.sleep(2000);
			}
			long endTime = System.currentTimeMillis();
			System.out.println(endTime - startTime + "ms");
			System.out
					.println("-------------------end-------------------------");
			/*
			 * File f = new File("F:\\work\\eclipse.rar");
			 * System.out.println("length:" + f.length());
			 */
		} catch (Exception e) {
			System.out.println("异常");
			e.printStackTrace();
		}
	}
}