package org.learningstorm.logweb.services;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import org.learningstorm.logweb.services.resources.CassandraUtils;
import org.learningstorm.logweb.services.resources.LogCount;

@ApplicationPath("/")
public class LogServices extends Application {
	public LogServices(){
		CassandraUtils.initCassandra();
	}
	
	@Override
	public Set<Class<?>> getClasses() {
		final Set<Class<?>> classes = new HashSet<Class<?>>();
		
		classes.add(LogCount.class);
		
		return classes;
	}
}
