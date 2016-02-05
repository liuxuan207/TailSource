package com.gaea.tailsource;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.gaea.tailsource.common.DateUtil;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        assertTrue( true );
    }
    
    public static void main(String[] args) throws IOException, ParseException{
    	File f = new File("src/test/java/t.txt");
    	System.out.println("f.exists:" + f.exists());
		System.out.println("f.isDirectory:" + f.isDirectory());
		System.out.println(System.getProperty("os.name"));
		System.out.println("f.canWrite:" + f.canWrite());
		System.out.println("f.getAbsolutePath:" + f.getAbsolutePath());
		System.out.println(f.getCanonicalPath());
		System.out.println(f.getName());
		SimpleDateFormat Dspdf = new SimpleDateFormat("yyyyMMdd");
		Date d = Dspdf.parse("20151230");//Calendar.getInstance()
		d.setDate(d.getDate() + 1);
		System.out.println(Dspdf.format(d));
		//System.out.println(Files.getAttribute(f.toPath(), "unix:ino"));
    }
}
