package org.learningstorm.lambda.spout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

public class UserInfoSpout implements IBatchSpout {
	private static final long serialVersionUID = 1L;

	private static final String ALPHAS = "abcdefghijklmnopqrstuvwxyz";
	private static final String DIGITS = "0123456789";
	private static final String[] PROVINCES = {
		"BJ", "SH", "JS", "AH", "ZJ"
	};
	
	private static final String[][] CITIES = {
		{
			"Beijing" 
		},
		{
			"Shanghai" 
		},
		{
			"Nanjing", "Wuxi", "Xuzhou", "Changzhou", "Suzhou", "Nantong",
			"Liangyungang", "Huaian", "Yancheng", "Yangzhou", "Zhenjiang",
			"Taizhou", "Suqian"	
		},
		{
			"Hefei", "Wuhu", "Bengbu", "Huaian", "Maanshan", "Huaibei",
				"Tongling", "Anqing", "Huangshan", "Fuyang", "Suzhou",
				"Chuzhou", "Luan", "Xuancheng", "Chizhou", "Bozhou"
		},
		{
			"Hangzhou", "Ningbo", "Wenzhou", "Shaoxing", "Huzhou", 
			"Jiaxing", "Jinhua", "Quzhou", "Taizhou", "Lishui", "Zhoushan"
		}
	};
	
	static Fields FIELDS = new Fields("name", "telephone", "email",
			"age", "province", "city");
	int maxBatchSize = 0;
	HashMap<Long, List<List<Object>>> batches = new HashMap<>();
	
	public UserInfoSpout(int maxBatchSize) {
		this.maxBatchSize = maxBatchSize;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context) {
	}

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		List<List<Object>> batch = this.batches.get(batchId);
		if ( batch == null ) {
			batch = new ArrayList<List<Object>>();
			
			for ( int i = 0; i < maxBatchSize; i ++ ) {
				batch.add(nextTuple());
			}
			
			this.batches.put(batchId, batch);
		}
		
		for ( List<Object> list : batch ) {
			collector.emit(list);
		}
	}

	private List<Object> nextTuple() {
		String name = randomString(ALPHAS, 3, 8);
		String telephone = randomString(DIGITS, 11, 11);
		String email = name + "@test.com";
		
		int provinceIndex = randomInteger(PROVINCES.length);
		String province = PROVINCES[provinceIndex];
		
		String[] citiesOfProvince = CITIES[provinceIndex];
		int cityIndex = randomInteger(citiesOfProvince.length);
		String city = citiesOfProvince[cityIndex];
		
		int age = randomInteger(10, 50);
		
		return new Values(name, telephone, email, age, province, city);
	}
	
	private String randomString(String characters, 
			int minLength, int maxLength) {
		int stringLength = randomInteger(minLength, maxLength);
		StringBuffer buffer = new StringBuffer();
		
		for ( int i = 0; i < stringLength; i ++ ) {
			int charIndex = randomInteger(characters.length());
			buffer.append(characters.charAt(charIndex));
		}
		
		return buffer.toString();
	}

	private int randomInteger(int max) {
		return randomInteger(0, max);
	}

	private int randomInteger(int min, int max) {
		return min + (int)(Math.random() * (max - min));
	}

	@Override
	public void ack(long batchId) {
		this.batches.remove(batchId);
	}

	@Override
	public void close() {
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Map getComponentConfiguration() {
		Config conf = new Config();
		conf.setMaxTaskParallelism(1);
		
		return conf;
	}

	@Override
	public Fields getOutputFields() {
		return FIELDS;
	}
}
