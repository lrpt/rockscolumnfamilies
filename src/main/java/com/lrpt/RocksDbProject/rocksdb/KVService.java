package com.lrpt.RocksDbProject.rocksdb;

import java.util.Optional;

public interface KVService<K, V> {

	boolean createTable(Object schema) throws Exception;

	boolean put(Object PutObject) throws Exception;

	Optional<V> get(K key) throws Exception;

	boolean delete(K key) throws Exception;

}
