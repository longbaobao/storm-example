package org.learningstorm.lambda;

public class TypeMismatchedException extends Exception {
	private static final long serialVersionUID = -6433528796608719493L;
	
	public TypeMismatchedException(String fieldName) {
		setFieldName(fieldName);
	}
	
	@Override
	public String getLocalizedMessage() {
		return String.format("The type of field '%s' is mismatched", 
				fieldName);
	}

	@Override
	public String getMessage() {
		return getLocalizedMessage();
	}

	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}
	
	private String fieldName;
}