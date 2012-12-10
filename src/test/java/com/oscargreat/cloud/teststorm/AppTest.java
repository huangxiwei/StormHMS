package com.oscargreat.cloud.teststorm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oscargreat.cloud.stormhm.model.CheckReply;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase {
	/**
	 * Create the test case
	 * 
	 * @param testName
	 *            name of the test case
	 */
	public AppTest(String testName) {
		super(testName);
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite() {
		return new TestSuite(AppTest.class);
	}

	/**
	 * Rigourous Test :-)
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public void testApp() throws IOException, InterruptedException {
		TestAgent ta = new TestAgent();
		ta.init();
		List<String> list = new ArrayList<String>();
		list.add("http://localhost:9998/hostMonitorAPI/metric/1234");
		list.add("http://localhost:9998/hostMonitorAPI/metric/4456");
		list.add("http://localhost:9998/hostMonitorAPI/metric/5677");
		String id = ta.startQuery(list);
		System.out.println("JOB ID: "+ id);
		
		ObjectMapper map = new ObjectMapper();
		for(int i = 0 ;i < 3; i++){
			Thread.sleep(500);
			CheckReply re = ta.checkProgress(id);
			System.out.println("reply: "+ map.writeValueAsString(re));
		}
		ta.close();
	}
}
