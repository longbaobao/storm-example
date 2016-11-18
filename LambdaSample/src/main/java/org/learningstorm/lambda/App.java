package org.learningstorm.lambda;

import java.util.List;
import java.util.UUID;

import org.learningstorm.lambda.dao.UserDao;
import org.learningstorm.lambda.model.User;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class App 
{
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
			System.err.printf("Datcenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}
		
		session = cluster.connect();
	}
	
	public void close() {
		session.close();
		cluster.close();
	}
	
	public void query(UUID dataId) throws TypeMismatchedException {
		UserDao userDao = new UserDao(session);
		User user = userDao.find(dataId);
		
		System.err.println(user);
	}
	
	public void query(String userName) throws TypeMismatchedException {
		UserDao userDao = new UserDao(session);
		List<User> users = userDao.findByName(userName);
		
		System.err.println(users.size());
		System.err.println(users.get(0));
	}
	
	public UUID create() throws TypeMismatchedException {
		UserDao userDao = new UserDao(session);
		User user = new User();
		user.setName("zhanghua");
		user.setTelephone("+86-13312567312");
		user.setEmail("zhanghua@tt.com");
		user.setProvince("Anhui");
		user.setCity("Hefei");
		user.setAge(42);
		
		userDao.create(user);
		
		user.setEmail("zhanghua@test.com");
		user.setAge(43);
		
		userDao.update(user);
		
		user.setProvince("Jiangsu");
		user.setCity("Nanjing");
		
		userDao.update(user);
		
		return user.getDataId();
	}
	
    public static void main( String[] args )
    {
    	App app = new App();
    	app.connect("127.0.0.1");
    	UUID dataId;
		try {
			dataId = app.create();
	    	app.query(dataId);
	    	app.query("zhanghua");
		} catch (TypeMismatchedException e) {
			e.printStackTrace();
		}
		finally {
			app.close();
		}
    }
}
