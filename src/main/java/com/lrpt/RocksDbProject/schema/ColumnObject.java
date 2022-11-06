package com.lrpt.RocksDbProject.schema;

public class ColumnObject {

	private String name;
	private Object value;


	public ColumnObject() {

	}


	public void setName(String columnName) {

		name = columnName;
	}


	public void setValue(Object columnValue) {

		value = columnValue;
	}


	public String getName() {

		return name;
	}


	public Object getValue() {

		return value;
	}

}
