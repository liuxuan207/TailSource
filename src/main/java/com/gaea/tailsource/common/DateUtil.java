package com.gaea.tailsource.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
	private SimpleDateFormat Dspdf = new SimpleDateFormat("yyyyMMdd");
	private DateUtil() {
	}
	public static DateUtil newInstance(){
		return new DateUtil();
	}
	
	public String nowDay(){
		return Dspdf.format(new Date(System.currentTimeMillis()));
	}
	
	@SuppressWarnings("deprecation")
	public String incDay(String day, int incDay){
		try {
			Date d = Dspdf.parse(day);
			d.setDate(d.getDate() + incDay);
			return Dspdf.format(d);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public boolean diffDay(long comparedTime){
		return !nowDay().equals(Dspdf.format(new Date(comparedTime)));
	}
	
	public boolean diffDay(String today, long comparedTime){
		return !today.equals(Dspdf.format(new Date(comparedTime)));
	}
	
	public boolean diffDay(String comapredDay){
		return !nowDay().equals(comapredDay);
	}
	
	public static boolean diffTime(long comparedTime, long diff){
		long now = System.currentTimeMillis();
		return now - comparedTime > diff ? true : false;
	}
}
