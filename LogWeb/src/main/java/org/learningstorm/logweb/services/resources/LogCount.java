package org.learningstorm.logweb.services.resources;

import java.util.Iterator;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.CounterSlice;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceCounterQuery;

@Path("/LogCount")
public class LogCount {
	@SuppressWarnings("unchecked")
	@GET
	@Path("/TotalsForMinute/{timestamp}")
	@Produces("application/json")
	public String getMinuteTotals(@PathParam("timestamp") String timestamp) {
		SliceCounterQuery<String, String> query = HFactory.createCounterSliceQuery(
				CassandraUtils.keyspace, StringSerializer.get(), StringSerializer.get());
		
		query.setColumnFamily("LogVolumeByMinute");
		query.setKey(timestamp);
		query.setRange("", "", false, 100);
		
		QueryResult<CounterSlice<String>> result = query.execute();
		
		Iterator<HCounterColumn<String>> it = result.get().getColumns().iterator();
		JSONArray content = new JSONArray();
		while ( it.hasNext() ) {
			HCounterColumn<String> column = it.next();
			JSONObject fileObject = new JSONObject();
			fileObject.put("FileName", column.getName());
			fileObject.put("Total", column.getValue());
			fileObject.put("Minute", Long.parseLong(timestamp));
			content.add(fileObject);
		}
		
		return content.toJSONString();
	}
}
