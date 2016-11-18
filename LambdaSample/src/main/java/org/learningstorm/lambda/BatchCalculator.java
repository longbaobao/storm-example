package org.learningstorm.lambda;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.learningstorm.lambda.dao.UserDao;
import org.learningstorm.lambda.model.User;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class BatchCalculator {
	private Cluster cluster;
	private Session session;
	
	public Session getSession() {
		return session;
	}
	
	public void connect(String node) {
		cluster = Cluster.builder()
				.addContactPoint(node)
				.build();
		
		Metadata metadata = cluster.getMetadata();
		System.err.printf("Connected to cluster: %s\n", metadata.getClusterName());
		for ( Host host : metadata.getAllHosts() ) {
			System.err.printf("Datacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}
		
		session = cluster.connect();
	}
	
	public void close() {
		session.close();
		cluster.close();
	}
	
	public void calculate() throws TypeMismatchedException {
		UserDao userDao = new UserDao(session);
		List<User> users = userDao.findAll();
		
		System.err.println(users.size());
		
		Map<String, Integer> provinceCounters = new HashMap<>();
		Map<String, Integer> provinceCityCounters = new HashMap<>();
		
		for ( User user : users ) {
			String province = user.getProvince();
			String city = user.getCity();
			String provinceCity = combinePair(province, city);
			
			increaseCounter(provinceCounters, province);
			increaseCounter(provinceCityCounters, provinceCity);
		}
		
		int currentTimestampBoundary = Util.getTimestampBoundary();
		
		persistProvinceCounters(provinceCounters, currentTimestampBoundary);
		persistProvinceCityCounters(provinceCityCounters, currentTimestampBoundary);
	}

	private void persistProvinceCityCounters(
			Map<String, Integer> provinceCityCounters,
			int currentTimestampBoundary) {
		getSession().execute("TRUNCATE lambdasample.province_city_count_batch");
		
		for ( Map.Entry<String, Integer> pair : provinceCityCounters.entrySet() ) {
			String stmt = "INSERT INTO "
					+ "lambdasample.province_city_count_batch "
					+ "(begin_time, province, city, count) "
					+ "values (?, ?, ?, ?)";
			
			PreparedStatement statement = getSession().prepare(stmt);
			BoundStatement boundStatement = new BoundStatement(statement);
			boundStatement.setInt(0, currentTimestampBoundary);
			
			String[] cityProvince = splitPair(pair.getKey());
			boundStatement.setString(1, cityProvince[0]);
			boundStatement.setString(2, cityProvince[1]);
			boundStatement.setInt(3, pair.getValue());
			
			getSession().execute(boundStatement);
		}
	}

	private String[] splitPair(String key) {
		return key.split(" ");
	}

	private void persistProvinceCounters(Map<String, Integer> provinceCounters,
			int currentTimestampBoundary) {
		getSession().execute("TRUNCATE lambdasample.province_count_batch");
		
		for ( Map.Entry<String, Integer> pair : provinceCounters.entrySet() ) {
			String stmt = "INSERT INTO "
					+ "lambdasample.province_count_batch "
					+ "(begin_time, province, count) "
					+ "values (?, ?, ?)";
			
			PreparedStatement statement = getSession().prepare(stmt);
			BoundStatement boundStatement = new BoundStatement(statement);
			boundStatement.setInt(0, currentTimestampBoundary);
			boundStatement.setString(1, pair.getKey());
			boundStatement.setInt(2, pair.getValue());
			
			getSession().execute(boundStatement);
		}
	}

	private void increaseCounter(Map<String, Integer> counters, String key) {
		if ( !counters.containsKey(key) ) {
			counters.put(key, 0);
		}
		
		counters.put(key, counters.get(key) + 1);
	}

	private String combinePair(String province, String city) {
		return province + " " + city;
	}
	
	public static void main(String[] args) {
		BatchCalculator calculator = new BatchCalculator();
		calculator.connect("127.0.0.1");
		
		try {
			calculator.calculate();
		} catch (TypeMismatchedException e) {
			e.printStackTrace();
		}
		finally {
			calculator.close();
		}
	}
}
