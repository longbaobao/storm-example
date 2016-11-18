package org.learningstorm.lambda.function;

import java.util.Map;

import org.learningstorm.lambda.TypeMismatchedException;
import org.learningstorm.lambda.dao.UserDao;
import org.learningstorm.lambda.model.User;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.hmsonline.trident.cql.MapConfiguredCqlClientFactory;

import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class CassandraPersistent extends BaseFilter {
	private static final long serialVersionUID = 1L;

	private Cluster cluster;
	private Session session;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		super.prepare(conf, context);
		
		String node = (String)conf.get(MapConfiguredCqlClientFactory.TRIDENT_CASSANDRA_CQL_HOSTS);
		connectCassandra(node);
	}

	private void connectCassandra(String node) {
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

	@Override
	public void cleanup() {
		closeCassandra();
		
		super.cleanup();
	}
	
	private void closeCassandra() {
		session.close();
		cluster.close();
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		UserDao userDao = new UserDao(session);
		User user = new User();
		
		user.setName(tuple.getString(0));
		user.setTelephone(tuple.getString(1));
		user.setEmail(tuple.getString(2));
		user.setAge(tuple.getInteger(3));
		user.setProvince(tuple.getString(4));
		user.setCity(tuple.getString(5));
		
		try {
			userDao.create(user);
		} catch (TypeMismatchedException e) {
			e.printStackTrace();
			
			return false;
		}
		
		return true;
	}

}
