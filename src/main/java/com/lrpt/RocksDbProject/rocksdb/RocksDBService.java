package com.lrpt.RocksDbProject.rocksdb;

import java.util.Optional;

import javax.annotation.PostConstruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lrpt.RocksDbProject.Exception.WrongInputException;
import com.lrpt.RocksDbProject.schema.ColumnObject;
import com.lrpt.RocksDbProject.schema.Table;
import com.lrpt.RocksDbProject.schema.TableObject;

@Service
public class RocksDBService implements KVService<Object, Object> {

	static Logger log = LogManager.getLogger();
	@Autowired
	RocksDatabase rocks;


	@PostConstruct
	void initialize() throws RocksDBException {

		rocks.init();
	}


	@Override
	public synchronized boolean createTable(Object schema) throws Exception {

		ObjectMapper objectMapper = new ObjectMapper();
		try {
			Table table = objectMapper.readValue((String) schema, Table.class);
			rocks.createTable(table);
		} catch (JsonProcessingException e) {
			log.error(e.getMessage());
			throw new WrongInputException("Not possible to create the table. Review your input");
		}
		return true;
	}


	@Override
	public boolean put(Object putObject) throws Exception {

		ObjectMapper objectMapper = new ObjectMapper();
		TableObject to = objectMapper.readValue((String) putObject, TableObject.class);
		return rocks.put(to);
	}


	@Override
	public Optional<Object> get(Object getObject) throws Exception {

		ObjectMapper objectMapper = new ObjectMapper();
		TableObject to = objectMapper.readValue((String) getObject, TableObject.class);
		TableObject value = null;
		value = rocks.get(to);
		return value != null ? Optional.of(convertToString(value)) : Optional.empty();
	}


	@Override
	public boolean delete(Object deleteObject) throws Exception {

		ObjectMapper objectMapper = new ObjectMapper();
		TableObject to = objectMapper.readValue((String) deleteObject, TableObject.class);
		return rocks.delete(to);
	}


	private String convertToString(TableObject tableObject) {

		JSONObject obj = new JSONObject();
		for (ColumnObject column : tableObject.getColumns()) {
			obj.append(column.getName(), column.getValue());
		}
		return obj.toString();
	}

}