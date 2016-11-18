package org.learningstorm.tfidf.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Values;
import storm.trident.tuple.TridentTuple;

public class OuterJoinState {
	private static Logger log = LoggerFactory.getLogger(OuterJoinState.class);
	private HashMap<Integer, List<Object[]>> bothSides = new HashMap<>();

	public void addValues(int streamId, TridentTuple input) {
		if ( !bothSides.keySet().contains(streamId) ) {
			if ( bothSides.keySet().size() >= 2) {
				throw new IllegalArgumentException(
						"Outer join can only be performed between 2 streams");
			}
		}
		bothSides.get(streamId).add(input.toArray());
	}
	
	private int getLhs() {
		int len = Integer.MAX_VALUE;
		int index = 0;
		
		for ( int id : bothSides.keySet() ) {
			if ( bothSides.get(id).size() < len ) {
				len = bothSides.get(id).size();
				index = id;
			}
		}
		
		return index;
	}
	
	private int getRhs(int lhs) {
		for ( int test : bothSides.keySet() ) {
			if ( test != lhs ) {
				return test;
			}
		}
		
		throw new IllegalArgumentException("Can't find RHS!");
	}
	
	public List<Values> join() {
		List<Values> ret = new ArrayList<Values>();
		
		try {
			int lhsId = getLhs();
			int rhsId = getRhs(lhsId);
			
			for ( Object[] lhs : bothSides.get(lhsId) ) {
				for ( Object[] rhs : bothSides.get(rhsId) ) {
					Values vals = new Values(lhs);
					vals.addAll(Arrays.asList(rhs));
					ret.add(vals);
				}
			}
		}
		catch (Exception e) {
			log.error("Exception", e);
		}
		
		return ret;
	}
}
