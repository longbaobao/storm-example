package org.learningstorm.lambda.dao;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.learningstorm.lambda.TypeMismatchedException;
import org.learningstorm.lambda.model.User;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class UserDao extends BaseDao<User> {
	public UserDao(Session session) {
		super(session);
	}

	@Override
	protected String getTableName() {
		return "lambdasample.users";
	}

	@Override
	protected User rowToModel(Row row) {
		User user = new User();
		
		user.setName(row.getString("name"));
		user.setTelephone(row.getString("telephone"));
		user.setEmail(row.getString("email"));
		user.setProvince(row.getString("province"));
		user.setCity(row.getString("city"));
		user.setAge(row.getInt("age"));
		
		return user;
	}

	@Override
	protected Map<String, Object> modelToRow(User model) {
		Map<String, Object> row = new HashMap<String, Object>();
		
		row.put("name", model.getName());
		row.put("telephone", model.getTelephone());
		row.put("email", model.getEmail());
		row.put("province", model.getProvince());
		row.put("city", model.getCity());
		row.put("age", model.getAge());
		
		return row;
	}

	public List<User> findByName(String userName) throws TypeMismatchedException {
		return find(EQ("name", "zhanghua"));
	}

}
