package com.lrpt.RocksDbProject.schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.json.JSONException;

public class TableEncoderDecoder {

	private TableEncoderDecoder() {

	}


	public static byte[] getKeysFromTableObject(TableObject tableObject, Table table) throws Exception {

		return getFromTableObject(tableObject, table, true);
	}


	public static byte[] getValuesFromTableObject(TableObject tableObject, Table table) throws Exception {

		return getFromTableObject(tableObject, table, false);
	}


	private static byte[] getFromTableObject(TableObject tableObject, Table table, boolean isKey) throws Exception {

		List<Column> columns = table.getColumns();
		Map<String, ColumnObject> columnObjects = tableObject.getColumns().stream()
				.collect(Collectors.toMap(ColumnObject::getName, Function.identity()));
		try (ByteArrayOutputStream buff = new ByteArrayOutputStream();
				DataOutputStream dOut = new DataOutputStream(buff);) {

			for (Column column : columns) {
				if (column.isKey() == isKey) {
					encode(dOut, column, columnObjects.get(column.getName()).getValue());
				}
			}
			dOut.flush();

			return buff.toByteArray();

		}
	}


	private static void encode(DataOutputStream outputStream, Column column, Object value) throws Exception {

		switch (column.getType()) {

		case VARCHAR:

			String stringvalue = (String) value;
			byte[] valuBytes = stringvalue.getBytes("UTF-8");

			outputStream.writeInt(valuBytes.length);
			outputStream.write(valuBytes, 0, valuBytes.length);
			break;
		case BOOLEAN:
			boolean booleanValue = (boolean) value;
			outputStream.writeBoolean(booleanValue);
			break;
		case INT:
			int integerValue = (int) value;
			outputStream.writeInt(integerValue);
			break;
		case LONG:
			long longValue = (long) value;
			outputStream.writeLong(longValue);
		case DATE:
			Date dateValue = (Date) value;
			long time = dateValue.getTime();
			outputStream.writeLong(time);

		}

	}


	public static TableObject getKeysFromBytes(byte[] bytes, Table table) throws JSONException, Exception {

		return getFromBytes(bytes, table, true);

	}


	public static TableObject getValuesFromBytes(byte[] bytes, Table table) throws JSONException, Exception {

		return getFromBytes(bytes, table, false);

	}


	private static Object decode(DataInputStream InputStream, Column column) throws Exception {

		switch (column.getType()) {

		case VARCHAR:
			int stringLenght = InputStream.readInt();
			ByteBuffer bb = ByteBuffer.allocate(stringLenght);

			for (int i = 0; i < stringLenght; i++) {
				bb.put(InputStream.readByte());
			}
			return new String(bb.array(), "UTF-8");

		case BOOLEAN:
			return InputStream.readBoolean();

		case INT:
			return InputStream.readInt();

		case LONG:
			return InputStream.readLong();

		case DATE:
			long longValue = InputStream.readLong();
			return new Date(longValue);

		default:
			return "";

		}

	}


	private static TableObject getFromBytes(byte[] bytes, Table table, boolean isKey) throws Exception {

		List<Column> columns = table.getColumns();
		TableObject tableObject = new TableObject();
		tableObject.setName(table.getName());
		List<ColumnObject> columnObjects = new ArrayList<>();
		tableObject.setColumns(columnObjects);
		try (ByteArrayInputStream buff = new ByteArrayInputStream(bytes);
				DataInputStream dIS = new DataInputStream(buff)) {

			for (Column column : columns) {
				if (column.isKey() == isKey) {
					ColumnObject columnObject = new ColumnObject();
					columnObject.setName(column.getName());
					columnObject.setValue(decode(dIS, column));
					columnObjects.add(columnObject);
				}
			}

		}

		return tableObject;
	}

}
