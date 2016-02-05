package com.gaea.tailsource;

import org.apache.flume.source.taildir.TaildirSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * App main.
 *
 */
public class App 
{	
	private static final Logger logger = LoggerFactory
			.getLogger(App.class);
	public static String getOs(){
		String os = System.getProperty("os.name").toLowerCase();
		if (os.startsWith("win")){
			return "windows";
		}else {
			return "*nix";
		}
	}
	
    public static void main( String[] args )
    {	
    	//args = new String[] {"src/test/java/config.properties"};
    	args = new String[] {"config/config.properties"};
    	String configfile = System.getProperty("config");
    	if (configfile == null && args.length == 0){
    		logger.error("config file not specified. exit.");
    		System.exit(1);
    	}else if (configfile == null){
    		System.setProperty("config", args[0]);
    	}
    	
    	if (System.getProperty("config") == null){
    		logger.error("config file is null. exit.");
    		System.exit(1);
    	}
    	
    	TaildirSource tailSource = new TaildirSource();
		tailSource.start();
		while (tailSource.shouldContinue()){
			logger.info("[Main] continue to tail the nextDay...");
			if (tailSource.updateConfig(true)){
				tailSource.start();
			}
		}
		logger.info("[Main] app Exit...");
		System.exit(0);
    }
}
