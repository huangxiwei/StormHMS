package com.oscargreat.cloud.stormhm.wrapper;

public class Out {
	public static void print(String s){
		System.out.println(s);
	}
	public static void print(String s, Object...args){
		System.out.printf(s,args);
	}
}
