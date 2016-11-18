package org.learningstorm.logweb.services.resources;

import java.io.IOException;
import java.util.Properties;

import org.learningstorm.logweb.services.Main;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

public class CassandraUtils {
	public static Cluster cluster;
	public static Keyspace keyspace;
	protected static Properties properties;
	
	public static boolean initCassandra() {
		properties = new Properties();
		try {
			properties.load(Main.class.getResourceAsStream("/cassandra.properties"));
		}
		catch ( IOException e ) {
			e.printStackTrace();
		}
		
		cluster = HFactory.getOrCreateCluster(properties.getProperty("cluster.name", "DefaultCluster"),
				properties.getProperty("cluster.hosts", "127.0.0.1:9160"));
		
		ConfigurableConsistencyLevel ccl = new ConfigurableConsistencyLevel();
		ccl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
		
		String keyspaceName = properties.getProperty("logging.keyspace", "Logging");
		
		keyspace = HFactory.createKeyspace(keyspaceName, cluster, ccl);
		
		return (cluster.describeKeyspace(keyspaceName) != null);		
	}
}
