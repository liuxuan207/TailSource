package com.gaea.tailsource.sender;

import java.util.Random;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class CustomPartitioner implements Partitioner {
	private Random rnd = new Random();
	
	public CustomPartitioner(VerifiableProperties props) {
    }

	@Override
	public int partition(Object key, int numPartitions) {
		if (key instanceof Integer){
			return ((Integer) key) % numPartitions;
		}
		return rnd.nextInt(numPartitions);
	}

}
