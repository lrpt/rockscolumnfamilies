package com.lrpt.RocksDbProject.rocksdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.SerializationUtils;

import com.lrpt.RocksDbProject.Exception.ApplicationException;
import com.lrpt.RocksDbProject.Exception.ResourceNotFoundException;
import com.lrpt.RocksDbProject.Exception.WrongInputException;
import com.lrpt.RocksDbProject.schema.Column;
import com.lrpt.RocksDbProject.schema.Table;
import com.lrpt.RocksDbProject.schema.TableEncoderDecoder;
import com.lrpt.RocksDbProject.schema.TableObject;

@Component
public class RocksDatabase {

	public static Logger log = LogManager.getLogger();

	private RocksDB db;
	private HashMap<String, Table> tables;
	@Autowired
	private RocksAggregatedOptions rocksAggregatedOptions;
	private List<ColumnFamilyHandle> columnFamilyHandleList;

	static {
		RocksDB.loadLibrary();
	}


	public void init() throws RocksDBException {

		List<ColumnFamilyDescriptor> cfDescriptors = getColumnFamilies();
		columnFamilyHandleList = new ArrayList<>();
		db = RocksDB.open(rocksAggregatedOptions.getDBOptions(), rocksAggregatedOptions.getBaseDir().getAbsolutePath(),
				cfDescriptors, columnFamilyHandleList);
		setTables(cfDescriptors);
		log.info("RocksDB initialized");

	}


	/*
	 * Get table schema from default column family and put in tables map
	 */
	private void setTables(List<ColumnFamilyDescriptor> columnFamilyDescriptors) {

		tables = new HashMap<>();
		columnFamilyDescriptors.stream().filter(cfd -> !Arrays.equals(cfd.getName(), RocksDB.DEFAULT_COLUMN_FAMILY))
				.forEach(cfd -> {
					try {
						tables.put(new String(cfd.getName()),
								(Table) SerializationUtils.deserialize(db.get(cfd.getName())));
					} catch (RocksDBException e) {
						// TODO review error
						e.printStackTrace();
					}
				});
	}


	/*
	 * Get current column families. This is needed because to open db we need to
	 * provide current cfs
	 */
	private List<ColumnFamilyDescriptor> getColumnFamilies() throws RocksDBException {

		List<ColumnFamilyDescriptor> cfDescriptors;
		Options options = new Options();
		List<byte[]> columnFamilies = RocksDB.listColumnFamilies(options,
				rocksAggregatedOptions.getBaseDir().getAbsolutePath());

		cfDescriptors = getColumnFamiliesDescriptors(columnFamilies);

		return cfDescriptors;
	}


	private List<ColumnFamilyDescriptor> getColumnFamiliesDescriptors(List<byte[]> columnFamilies) {

		List<ColumnFamilyDescriptor> cfDescriptors;
		cfDescriptors = columnFamilies.stream().map(nameBytes -> {
			return new ColumnFamilyDescriptor(nameBytes, rocksAggregatedOptions.getColumnFamilyOptions());
		}).collect(Collectors.toList());
		if (cfDescriptors.isEmpty()) {
			cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,
					rocksAggregatedOptions.getColumnFamilyOptions()));
		}
		return cfDescriptors;
	}


	public boolean createTable(Table table) {

		if (tables.containsKey(table.getName())) {
			throw new WrongInputException("Table already exists");
		}
		tableSchemaHasKeys(table);
		ColumnFamilyDescriptor newTable = new ColumnFamilyDescriptor(table.getName().getBytes(),
				rocksAggregatedOptions.getColumnFamilyOptions());
		try {
			columnFamilyHandleList.add(db.createColumnFamily(newTable));
			registerNewTable(table);
		} catch (RocksDBException e) {

			throw new ApplicationException("Error Creating table", e);
		}
		return true;
	}


	private void registerNewTable(Table table) throws RocksDBException {

		db.put(table.getName().getBytes(), SerializationUtils.serialize(table));
		tables.put(table.getName(), table);
	}


	private void tableSchemaHasKeys(Table table) {

		for (Column column : table.getColumns()) {
			if (column.isKey()) {
				return;
			}
		}
		throw new WrongInputException("Table does not contain keys");

	}


	public boolean put(TableObject tableObject) throws Exception {

		Table table = tables.get(tableObject.getName());
		tableExists(table);
		if (!table.validSchema(tableObject)) {
			throw new WrongInputException("Table schema does not match with provided data");
		}

		byte[] key = TableEncoderDecoder.getKeysFromTableObject(tableObject, table);
		byte[] value = TableEncoderDecoder.getValuesFromTableObject(tableObject, table);
		try {
			db.put(getColumnFamilyHandle(table.getName()), key, value);
		} catch (RocksDBException e) {
			throw new ApplicationException("Error inserting object in table", e);

		}
		return true;
	}


	public TableObject get(TableObject tableObject) throws Exception {

		Table table = tables.get(tableObject.getName());

		tableExists(table);
		validateIfHasKeys(tableObject, table);

		byte[] keys = TableEncoderDecoder.getKeysFromTableObject(tableObject, table);

		try {
			byte[] bytes = db.get(getColumnFamilyHandle(table.getName()), keys);
			if (bytes != null) {
				return TableEncoderDecoder.getValuesFromBytes(bytes, table);
			}
			return null;
		} catch (RocksDBException e) {
			throw new ApplicationException("Error getting object", e);
		}

	}


	private void validateIfHasKeys(TableObject tableObject, Table table) {

		if (!table.validKeys(tableObject)) {
			throw new WrongInputException("Table keys were not provided");
		}
	}


	public boolean delete(TableObject tableObject) throws Exception {

		Table table = tables.get(tableObject.getName());

		tableExists(table);
		validateIfHasKeys(tableObject, table);

		byte[] keys = TableEncoderDecoder.getKeysFromTableObject(tableObject, table);
		try {
			db.delete(getColumnFamilyHandle(table.getName()), keys);
			db.dropColumnFamily(getColumnFamilyHandle(table.getName()));
			columnFamilyHandleList.remove(getColumnFamilyHandle(table.getName()));
			tables.remove(table.getName());
			return true;
		} catch (RocksDBException e) {
			throw new ApplicationException("Error deleting object", e);
		}

	}


	private void tableExists(Table table) {

		if (table == null) {
			throw new ResourceNotFoundException("Table does not exist");
		}
	}


	public ColumnFamilyHandle getColumnFamilyHandle(String name) {

		return columnFamilyHandleList.stream().filter(handle -> {
			try {
				return Arrays.equals(handle.getName(), name.getBytes());
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}).findAny().orElse(null);
	}


	public void close() throws RocksDBException {

		db.pauseBackgroundWork();

		for (ColumnFamilyHandle cfh : columnFamilyHandleList) {
			cfh.close();
		}
		db.closeE();
		rocksAggregatedOptions.close();
	}

}
