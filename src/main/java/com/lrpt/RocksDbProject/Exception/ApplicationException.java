package com.lrpt.RocksDbProject.Exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR)
public class ApplicationException extends RuntimeException {

	private static final long serialVersionUID = 1L;


	public ApplicationException(String message) {

		super(message);
	}


	public ApplicationException(String message, Throwable cause) {

		super(message, cause);
	}

}
