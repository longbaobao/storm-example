package org.learningstorm.tfidf.state;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import backtype.storm.utils.Time;
import trident.cassandra.CassandraState.Options;
import trident.cassandra.RowKeyStrategy;

public class TimeBasedRowStrategy implements RowKeyStrategy, Serializable{
	private static final long serialVersionUID = -6040768755709327053L;

	@Override
	public <T> String getRowKey(List<List<Object>> keys, Options<T> options) {
		return options.rowKey + StateUtils.formatHour(new Date(Time.currentTimeMillis()));
	}

}
