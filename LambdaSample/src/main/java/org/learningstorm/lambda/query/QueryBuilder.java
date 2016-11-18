package org.learningstorm.lambda.query;

import java.util.ArrayList;
import java.util.List;

public class QueryBuilder {
	public String getStatement() {
		return statement;
	}
	
	public QueryBuilder setStatement(String statement) {
		this.statement = statement;
		
		return this;
	}
	
	public List<String> getFieldNames() {
		return fieldNames;
	}
	
	public QueryBuilder addFieldName(String fieldName) {
		fieldNames.add(fieldName);
		
		return this;
	}
	
	public QueryBuilder addFieldNames(List<String> fieldNames) {
		fieldNames.addAll(fieldNames);
		
		return this;
	}

	public List<Object> getFieldValues() {
		return fieldValues;
	}

	public QueryBuilder addFieldValue(Object fieldValue) {
		fieldValues.add(fieldValue);
		
		return this;
	}

	public QueryBuilder addFieldValues(List<Object> fieldValues) {
		fieldValues.addAll(fieldValues);
		
		return this;
	}
	
	private String statement;
	private List<String> fieldNames = new ArrayList<String>();
	private List<Object> fieldValues = new ArrayList<Object>();
}
