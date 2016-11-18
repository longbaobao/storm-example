package org.learningstorm.lambda.mapper;

import java.io.Serializable;
import java.util.List;

import org.learningstorm.lambda.Util;

import storm.trident.tuple.TridentTuple;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.hmsonline.trident.cql.mappers.CqlRowMapper;

public class ProvinceBatchMapper implements CqlRowMapper<List<String>, Number>, Serializable {
	private static final long serialVersionUID = -6135029143676707055L;

	public static final String KEYSPACE_NAME = "lambdasample";
	public static final String TABLE_NAME = "province_count_batch";
	public static final String KEY_NAME_BEGIN_TIME = "begin_time";
	public static final String KEY_NAME_PROVINCE = "province";
	public static final String VALUE_NAME = "count";
	
	@Override
	public Statement map(TridentTuple arg0) {
		return null;
	}

	@Override
	public Statement map(List<String> keys, Number value) {
		Insert statement = QueryBuilder.insertInto(KEYSPACE_NAME, TABLE_NAME);
		statement.value(KEY_NAME_BEGIN_TIME, Util.getTimestampBoundary());
		statement.value(KEY_NAME_PROVINCE, keys.get(0));
		statement.value(VALUE_NAME, value);
		
		return statement;
	}

	@Override
	public Statement retrieve(List<String> keys) {
		Select statement = QueryBuilder.select()
				.column(KEY_NAME_PROVINCE)
				.column(VALUE_NAME)
				.from(KEYSPACE_NAME, TABLE_NAME);
		
		statement.where(QueryBuilder.eq(KEY_NAME_BEGIN_TIME, Util.getTimestampBoundary() - 1));
		statement.where(QueryBuilder.eq(KEY_NAME_PROVINCE, keys.get(0)));
		
		return statement;
	}

	@Override
	public Number getValue(Row row) {
		return (Number)row.getInt(VALUE_NAME);
	}

}
