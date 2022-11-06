package com.lrpt.RocksDbProject.rocksdb;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.WriteBufferManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

@Component
@PropertySource("classpath:application.properties")
public class RocksAggregatedOptions {

	private DBOptions dbOptions;
	private ColumnFamilyOptions cfOptions;
	private File baseDir;
	private LRUCache cache;
	private WriteBufferManager bufferManager;
	private final static String DIRECTORY = "tables_db";

	@Value("${app.rocksdb.block_cache_size}")
	private long cacheSize;
	@Value("${app.rocksdb.max_write_buffer_size}")
	private long writeBufferSize;
	@Value("${app.rocksdb.block_size}")
	private long blockSize;


	public RocksAggregatedOptions() {

		createDirectories();
		cache = new LRUCache(cacheSize);
		bufferManager = new WriteBufferManager(writeBufferSize, cache);
		setDBOptions();
		setColumnFamilies();
	}


	private void createDirectories() {

		baseDir = new File("/tmp/rocks", DIRECTORY);
		try {
			Files.createDirectories(baseDir.getAbsoluteFile().toPath());
		} catch (IOException e) {
			// TODO review error handling
			e.printStackTrace();
		}
	}


	private void setDBOptions() {

		dbOptions = new DBOptions();
		dbOptions.setCreateMissingColumnFamilies(true);
		dbOptions.setCreateIfMissing(true);
		dbOptions.setWriteBufferManager(bufferManager);

	}


	private void setColumnFamilies() {

		cfOptions = new ColumnFamilyOptions();
		cfOptions.setCompressionType(CompressionType.SNAPPY_COMPRESSION);
		BlockBasedTableConfig table = new BlockBasedTableConfig();
		table.setBlockSize(blockSize);
		table.setBlockCache(cache);
		// include filters in cache cap.
		table.setCacheIndexAndFilterBlocks(true);
		table.setPinL0FilterAndIndexBlocksInCache(true);
		cfOptions.setTableFormatConfig(table);

	}


	public DBOptions getDBOptions() {

		return dbOptions;
	}


	public ColumnFamilyOptions getColumnFamilyOptions() {

		return cfOptions;
	}


	public File getBaseDir() {

		return baseDir;
	}


	public void close() {

		cache.close();
		bufferManager.close();
		dbOptions.close();
		cfOptions.close();
	}

}
