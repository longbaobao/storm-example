package org.learningstorm.lambda.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.learningstorm.lambda.TypeMismatchedException;
import org.learningstorm.lambda.model.BaseModel;
import org.learningstorm.lambda.query.QueryBuilder;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public abstract class BaseDao<T extends BaseModel> {
	public BaseDao(Session session) {
		this.session = session;
	}

	public Session getSession() {
		return session;
	}

	public void setSession(Session session) {
		this.session = session;
	}
	
	public void create(T model) throws TypeMismatchedException {
		model.setId(UUID.randomUUID());
		model.setDataId(UUID.randomUUID());
		model.setTimestamp(System.currentTimeMillis());
		model.setDeleted(false);
		
		insertRecord(model);
	}
	
	public void update(T model) throws TypeMismatchedException {
		model.setId(UUID.randomUUID());
		model.setTimestamp(System.currentTimeMillis());
		model.setDeleted(false);
		
		insertRecord(model);
	}
	
	public void delete(T model) throws TypeMismatchedException {
		model.setId(UUID.randomUUID());
		model.setTimestamp(System.currentTimeMillis());
		model.setDeleted(true);
		
		insertRecord(model);
	}
	
	public T find(UUID dataId) {
		String stmt = String.format("SELECT * FROM %s "
				+ "WHERE dataid = ?", getTableName());
		
		PreparedStatement statement = getSession().prepare(stmt);
		BoundStatement boundStatement = new BoundStatement(statement);
		boundStatement.bind(dataId);
		
		ResultSet resultSet = getSession().execute(boundStatement);
		T model = null;
		for ( Row row : resultSet ) {
			if ( model == null || model.getTimestamp() < row.getLong("timestamp") ) {
				model = rowToCompleteModel(row);
			}
		}
		
		if ( !model.isDeleted() ) {
			return model;
		}
		
		return null;
	}
	
	public List<T> find(QueryBuilder queryBuilder) throws TypeMismatchedException {
		String stmt = String.format("SELECT * FROM %s WHERE %s",
				getTableName(), queryBuilder.getStatement());
		
		PreparedStatement statement = getSession().prepare(stmt);
		BoundStatement boundStatement = new BoundStatement(statement);
		bindFieldValues(boundStatement, 
				queryBuilder.getFieldNames(), 
				queryBuilder.getFieldValues());
		
		ResultSet resultSet = getSession().execute(boundStatement);
		Map<UUID, T> models = new HashMap<UUID, T>();
		for ( Row row : resultSet ) {
			T model = models.get(row.getUUID("dataid"));
			
			if ( model == null || model.getTimestamp() < row.getLong("timestamp") ) {
				model = rowToCompleteModel(row);
				models.put(model.getDataId(), model);
			}
		}
		
		List<T> existModels = new ArrayList<T>();
		for ( UUID key : models.keySet() ) {
			T model = models.get(key);

			if ( !model.isDeleted() ) {
				existModels.add(model);
			}
		}
		
		return existModels;
	}
	
	public List<T> findAll() throws TypeMismatchedException {
		String stmt = String.format("SELECT * FROM %s", getTableName());
		
		ResultSet resultSet = getSession().execute(stmt);
		Map<UUID, T> models = new HashMap<UUID, T>();
		for ( Row row : resultSet ) {
			T model = models.get(row.getUUID("dataid"));
			
			if ( model == null || model.getTimestamp() < row.getLong("timestamp") ) {
				model = rowToCompleteModel(row);
				models.put(model.getDataId(), model);
			}
		}
		
		List<T> existModels = new ArrayList<T>();
		for ( UUID key : models.keySet() ) {
			T model = models.get(key);

			if ( !model.isDeleted() ) {
				existModels.add(model);
			}
		}
		
		return existModels;
	}
	
	public static interface ModelUpdater<T> {
		public void update(T model);
	}
	
	public List<T> update(QueryBuilder queryBuilder,
			ModelUpdater<T> modelUpdater) throws TypeMismatchedException {
		List<T> models = find(queryBuilder);
		
		for ( T model : models ) {
			modelUpdater.update(model);
			update(model);
		}
		
		return models;
	}
	
	public void update(QueryBuilder queryBuilder) 
			throws TypeMismatchedException {
		List<T> models = find(queryBuilder);
		
		for ( T model : models ) {
			delete(model);
		}
	}
	
	private void insertRecord(T model) throws TypeMismatchedException {
		Map<String, Object> row = completeModelToRow(model);
		List<String> fieldNames = new ArrayList<String>();
		List<String> placeholders = new ArrayList<String>();
		List<Object> fieldValues = new ArrayList<Object>();
		
		for ( String fieldName : row.keySet() ) {
			Object fieldValue = row.get(fieldName);
			
			fieldNames.add(fieldName);
			placeholders.add("?");
			fieldValues.add(fieldValue);
		}
		
		String stmt = String.format("INSERT INTO %s (%s) VALUES (%s)",
				getTableName(),
				joinStringList(fieldNames, ","),
				joinStringList(placeholders, ","));
		System.err.println(stmt);
		
		PreparedStatement statement = getSession().prepare(stmt);
		BoundStatement boundStatement = new BoundStatement(statement);
		bindFieldValues(boundStatement, fieldNames, fieldValues);
		
		getSession().execute(boundStatement);
	}

	private void bindFieldValues(BoundStatement boundStatement,
			List<String> fieldNames, List<Object> fieldValues) throws TypeMismatchedException {
		int fieldPosition = 0;
		for ( Object fieldValue : fieldValues ) {
			if ( fieldValue instanceof Boolean ) {
				boundStatement.setBool(fieldPosition, (Boolean)fieldValue);
			}
			else if ( fieldValue instanceof Integer ) {
				boundStatement.setInt(fieldPosition, (Integer)fieldValue);
			}
			else if ( fieldValue instanceof Long ) {
				boundStatement.setLong(fieldPosition, (Long)fieldValue);
			}
			else if ( fieldValue instanceof String ) {
				boundStatement.setString(fieldPosition, (String)fieldValue);
			}
			else if ( fieldValue instanceof UUID ) {
				boundStatement.setUUID(fieldPosition, (UUID)fieldValue);
			}
			else {
				throw new TypeMismatchedException(
						fieldNames.get(fieldPosition));
			}
			fieldPosition ++;
		}
		
	}

	protected T rowToCompleteModel(Row row) {
		T model = rowToModel(row);
		
		model.setId(row.getUUID("id"));
		model.setDataId(row.getUUID("dataid"));
		model.setTimestamp(row.getLong("timestamp"));
		model.setDeleted(row.getBool("deleted"));
		
		return model;
	}
	
	protected Map<String, Object> completeModelToRow(T model) {
		Map<String, Object> row = modelToRow(model);
		
		row.put("id", model.getId());
		row.put("dataid", model.getDataId());
		row.put("timestamp", model.getTimestamp());
		row.put("deleted", model.isDeleted());
		
		return row;
	}
	
	protected QueryBuilder EQ(String fieldName, Object fieldValue) {
		QueryBuilder queryBuilder = new QueryBuilder();
		
		queryBuilder
			.setStatement(String.format("%s = ?", fieldName))
			.addFieldName(fieldName)
			.addFieldValue(fieldValue);
		
		return queryBuilder;
	}
	
	protected QueryBuilder NE(String fieldName, Object fieldValue) {
		QueryBuilder queryBuilder = new QueryBuilder();
		
		queryBuilder
			.setStatement(String.format("%s <> ?", fieldName))
			.addFieldName(fieldName)
			.addFieldValue(fieldValue);
		
		return queryBuilder;
	}
	
	protected QueryBuilder GT(String fieldName, Object fieldValue) {
		QueryBuilder queryBuilder = new QueryBuilder();
		
		queryBuilder
			.setStatement(String.format("%s > ?", fieldName))
			.addFieldName(fieldName)
			.addFieldValue(fieldValue);
		
		return queryBuilder;
	}
	
	protected QueryBuilder LT(String fieldName, Object fieldValue) {
		QueryBuilder queryBuilder = new QueryBuilder();
		
		queryBuilder
			.setStatement(String.format("%s < ?", fieldName))
			.addFieldName(fieldName)
			.addFieldValue(fieldValue);
		
		return queryBuilder;
	}
	
	protected QueryBuilder GE(String fieldName, Object fieldValue) {
		QueryBuilder queryBuilder = new QueryBuilder();
		
		queryBuilder
			.setStatement(String.format("%s >= ?", fieldName))
			.addFieldName(fieldName)
			.addFieldValue(fieldValue);
		
		return queryBuilder;
	}
	
	protected QueryBuilder LE(String fieldName, Object fieldValue) {
		QueryBuilder queryBuilder = new QueryBuilder();
		
		queryBuilder
			.setStatement(String.format("%s <= ?", fieldName))
			.addFieldName(fieldName)
			.addFieldValue(fieldValue);
		
		return queryBuilder;
	}
	
	protected QueryBuilder NOT(QueryBuilder expression) {
		expression.setStatement(String.format(
				"NOT (%s)", expression.getStatement()));
		
		return expression;
	}
	
	protected QueryBuilder AND(QueryBuilder lhs, QueryBuilder rhs) {
		QueryBuilder queryBuilder = new QueryBuilder();
		
		queryBuilder
			.setStatement(String.format(
					"(%s) AND (%s)",
					lhs.getStatement(),
					rhs.getStatement()))
			.addFieldNames(lhs.getFieldNames())
			.addFieldNames(rhs.getFieldNames())
			.addFieldValues(lhs.getFieldValues())
			.addFieldValues(rhs.getFieldValues());
		
		return queryBuilder;
	}
	
	protected QueryBuilder OR(QueryBuilder lhs, QueryBuilder rhs) {
		QueryBuilder queryBuilder = new QueryBuilder();
		
		queryBuilder
			.setStatement(String.format(
					"(%s) OR (%s)",
					lhs.getStatement(),
					rhs.getStatement()))
			.addFieldNames(lhs.getFieldNames())
			.addFieldNames(rhs.getFieldNames())
			.addFieldValues(lhs.getFieldValues())
			.addFieldValues(rhs.getFieldValues());
		
		return queryBuilder;
	}
	
	protected abstract String getTableName();
	protected abstract T rowToModel(Row row);
	protected abstract Map<String, Object> modelToRow(T model);

	private Object joinStringList(List<String> stringList, String seperator) {
		StringBuilder builder = new StringBuilder();
		
		int listLength = stringList.size();
		int currentIndex = 0;
		for ( String string : stringList ) {
			builder.append(string);
			
			currentIndex ++;
			if ( currentIndex < listLength ) {
				builder.append(seperator);
			}
		}
		
		return builder.toString();
	}
	
	private Session session;
}
