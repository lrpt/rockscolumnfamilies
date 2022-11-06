package com.lrpt.RocksDbProject.schema;

import java.io.Serializable;

public class Column implements Serializable {

	private static final long serialVersionUID = 1L;
	private String name;
	private ColumnType type;
	private boolean key;


	public Column() {

	}


	public String getName() {

		return name;
	}


	public void setName(String name) {

		this.name = name;
	}


	public ColumnType getType() {

		return type;
	}


	public void setType(ColumnType type) {

		this.type = type;
	}


	public boolean isKey() {

		return key;
	}


	public void setKey(boolean key) {

		this.key = key;
	}


	@Override
	public String toString() {

		return "Name: " + name;
	}

}
