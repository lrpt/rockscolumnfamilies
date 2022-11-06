package com.lrpt.RocksDbProject.api;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.lrpt.RocksDbProject.rocksdb.KVService;

@RestController
@RequestMapping("/api/rocksdb")
public class Api {

	private final KVService<Object, Object> service;


	public Api(KVService<Object, Object> service) {

		this.service = service;
	}


	@PostMapping(value = "/createtable", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Object> createEntity(@RequestBody String schema) throws Exception {

		return service.createTable(schema) ? ResponseEntity.ok("good")
				: ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
	}


	@PostMapping(value = "/put", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Object> save(@RequestBody String value) throws Exception {

		return service.put(value) ? ResponseEntity.ok(value)
				: ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
	}


	@GetMapping(value = "/get", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Object> get(@RequestBody String key) throws Exception {

		return ResponseEntity.of(service.get(key));
	}


	@DeleteMapping(value = "/delete", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Object> delete(@RequestBody String key) throws Exception {

		return service.delete(key) ? ResponseEntity.noContent().build()
				: ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
	}

}