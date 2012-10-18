package test;

import java.util.Random;

import data.Triple;

public class TestHashcodes {

	public static void main(String[] args) {
		
		int times = 10000000;
		Triple t = new Triple();
		Random r = new Random();
		t.setSubject(r.nextLong());
		t.setPredicate(r.nextLong());
		t.setObject(r.nextLong());
		
		long time = System.currentTimeMillis();
		for(int i = 0; i < times; ++i) {
			int a = Math.abs(t.toString().hashCode());
		}
		System.out.println("Time before=" + (System.currentTimeMillis() - time));
		
		time = System.currentTimeMillis();
		for(int i = 0; i < times; ++i) {
			int a = t.hashCode() & Integer.MAX_VALUE;
		}
		System.out.println("Time after=" + (System.currentTimeMillis() - time));
	}
}
