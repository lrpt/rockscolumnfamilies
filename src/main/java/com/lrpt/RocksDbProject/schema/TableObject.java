package com.lrpt.RocksDbProject.schema;

import java.util.List;

public class TableObject {

	public String name;
	public List<ColumnObject> columns;


	public TableObject() {

	}


	public String getName() {

		return name;
	}


	public void setName(String tableName) {

		this.name = tableName;
	}


	public List<ColumnObject> getColumns() {

		return columns;
	}


	public void setColumns(List<ColumnObject> columns) {

		this.columns = columns;
	}

}
