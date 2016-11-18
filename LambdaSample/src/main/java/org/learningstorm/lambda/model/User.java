package org.learningstorm.lambda.model;

public class User extends BaseModel {
	private String name;
	private String telephone;
	private String email;
	private String province;
	private String city;
	private int age;
	
	@Override
	public String toString() {
		return String.format("%s\n"
				+ "name: %s\n"
				+ "telephone: %s\n"
				+ "email: %s\n"
				+ "province: %s\n"
				+ "city: %s\n"
				+ "age: %d",
				super.toString(), 
				name, telephone, email, province, city, age);
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getTelephone() {
		return telephone;
	}
	
	public void setTelephone(String telephone) {
		this.telephone = telephone;
	}
	
	public String getEmail() {
		return email;
	}
	
	public void setEmail(String email) {
		this.email = email;
	}
	
	public String getProvince() {
		return province;
	}
	
	public void setProvince(String province) {
		this.province = province;
	}
	
	public String getCity() {
		return city;
	}
	
	public void setCity(String city) {
		this.city = city;
	}
	
	public int getAge() {
		return age;
	}
	
	public void setAge(int age) {
		this.age = age;
	}
}
