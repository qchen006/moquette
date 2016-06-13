package io.moquette.ha;

import java.io.Serializable;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import org.junit.After;
import org.junit.Test;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class HazelcastMap {

	/**
	 * start the function N times, N equals to the local variable totalInstance 
	 * @throws InterruptedException
	 */
	@Test
	public void testMapConcurrency() throws InterruptedException {
		int totalTimes = 1000;
		int totalInstance = 2;
		
		HazelcastInstance hz = Hazelcast.newHazelcastInstance();
		IMap<String, Value> map = hz.getMap("map");
		String key = "1";
		map.put(key, new Value());
		System.out.println("Starting");
		for (int k = 0; k < totalTimes; k++) {
			if (k % 100 == 0)
				System.out.println("At: " + k);
			Value value = map.get(key);
			Thread.sleep(10);
			value.amount++;
			map.put(key, value);
		}
		int val =  map.get(key).amount;
		System.out.println("Finished! Result = " + map.get(key).amount);
		assertThat(val, not(equalTo(totalTimes*totalInstance)));
	}

	static class Value implements Serializable {
		public int amount;
	}
	
	
	@Test
	public void testMapConcurrencyLock() throws InterruptedException {
		int totalTimes = 1000;
		int totalInstance = 2;
		
		HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        IMap<String, Value> map = hz.getMap( "map2" );
        String key = "1";
        map.put( key, new Value() );
        System.out.println( "Starting" );
        for ( int k = 0; k < totalTimes; k++ ) {
            map.lock( key );
            try {
                Value value = map.get( key );
                Thread.sleep( 10 );
                value.amount++;
                map.put( key, value );
            } finally {
                map.unlock( key );
            }
        }
        System.out.println( "Finished! Result = " + map.get( key ).amount );
	}
	
	
	@After
	public void cleanUp(){
		Hazelcast.shutdownAll();
	}
}
