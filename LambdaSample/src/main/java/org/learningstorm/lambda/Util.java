package org.learningstorm.lambda;

public class Util {
	public static Integer getTimestampBoundary() {
		return (int)(System.currentTimeMillis() /
				(24 * 3600 * 1000));
	}
}
