package com.oscargreat.cloud.stormhm.wrapper;

public interface IMemcache {
	void init();
	void close();
	void set(Object key, Object value);
	Object get(Object key);
}
