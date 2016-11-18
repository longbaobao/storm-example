package org.learningstorm.logweb.services;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import javax.ws.rs.core.UriBuilder;

import org.glassfish.grizzly.http.server.HttpServer;
import org.learningstorm.logweb.services.resources.CassandraUtils;

import com.sun.jersey.api.container.grizzly2.GrizzlyServerFactory;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;

public class Main {
	private static int getPort(int defaultPort) {
		Properties properties = new Properties();
		try {
			properties.load(Main.class.getResourceAsStream("/cassandra.properties"));
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		
		return Integer.parseInt(properties.getProperty("port", Integer.toString(defaultPort)));
	}
	
	private static URI getBaseURI() {
		return UriBuilder.fromUri("http://localhost").port(getPort(9998)).build();
	}
	
	public static final URI BASE_URI = getBaseURI();
	
	protected static HttpServer startServer() throws IOException {
		System.out.println("Starting grizzly...");
		
		ResourceConfig rc = new PackagesResourceConfig("org.learningstorm.logweb.services.resources");
		
		return GrizzlyServerFactory.createHttpServer(BASE_URI, rc);
	}
	
	public static void main(String[] args) throws IOException {
		if ( CassandraUtils.initCassandra() ) {
			HttpServer httpServer = startServer();
			
			System.out.println(String.format(
					"Jersey app started with WADL available at "
					+ "%sapplication.wadl\nHit enter to stop it...",
					BASE_URI, BASE_URI));
			System.in.read();
			httpServer.stop();
		}
		else {
			System.out.println("The service layer assumes that Keyspace and server are already in place.\n" +
					" Please run the topology first and ensure that the settings are correct.");
		}
	}
}
