package com.oscargreat.cloud.stormhm.wrapper;

import java.sql.Timestamp;
import java.util.Random;

public class MetricsDBMock implements MetricsDao {
	Random r = new Random();
	public MetricsDBMock() {
		r.setSeed(System.currentTimeMillis());
	}
	@Override
	public void storeMetrics(String metrics, Timestamp timestamp) {
		//nothing, emulate a operation delay
		try {

			Thread.sleep(r.nextInt(1000));
		} catch (InterruptedException e) {
		}
		
	}

}
