package com.gaea.tailsource.common;

public class CommonUtil {
	public static String getOs(){
		String os = System.getProperty("os.name").toLowerCase();
		if (os.startsWith("win")){
			return "windows";
		}else {
			return "*nix";
		}
	}
}
