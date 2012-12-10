package com.oscargreat.cloud.stormhm.wrapper;
import java.util.HashMap;

import com.oscargreat.cloud.stormhm.wrapper.IMemcache;


public class LocalMemcache implements IMemcache {
	HashMap<Object, Object> map;
	@Override
	public void init() {
		map = new HashMap<Object, Object>();
	}

	@Override
	public void close() {
		
	}

	@Override
	public void set(Object key, Object value) {
		map.put(key, value);
		
	}

	@Override
	public Object get(Object key) {
		return map.get(key);
	}

}
