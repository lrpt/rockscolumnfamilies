package com.lrpt.RocksDbProject.schema;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

public class Table implements Serializable {

	private static final long serialVersionUID = 1L;
	private String name;
	private List<Column> columns;


	public List<Column> getColumns() {

		return columns;
	}


	public void setColumns(List<Column> columns) {

		this.columns = columns;
	}


	public String getName() {

		return name;
	}


	public Table(String name, List<Column> columns) {

		this.name = name;
		this.columns = columns;

	}


	public Table() {

	}


	public void setName(String name) {

		this.name = name;
	}


	public boolean validSchema(TableObject tableObject) {

		List<String> columnNames = getColumnNamesFromTableObject(tableObject);

		for (Column column : columns) {
			if (!columnNames.contains(column.getName())) {
				return false;
			}
		}
		return true;
	}


	public boolean validKeys(TableObject tableObject) {

		List<String> columnNames = getColumnNamesFromTableObject(tableObject);

		for (Column column : columns) {
			if (column.isKey() && !columnNames.contains(column.getName())) {
				return false;
			}
		}
		return true;
	}


	private List<String> getColumnNamesFromTableObject(TableObject tableObject) {

		List<String> columnNames = tableObject.getColumns().stream().map(ColumnObject::getName)
				.collect(Collectors.toList());
		return columnNames;
	}


	@Override
	public String toString() {

		return name + columns.toString();
	}

}
