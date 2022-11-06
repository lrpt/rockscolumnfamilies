package com.lrpt.RocksDbProject.Exception;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

@ControllerAdvice
public class RestControllerExceptionHandler {

	private static final Logger log = LogManager.getLogger();


	@ExceptionHandler(ResourceNotFoundException.class)
	@ResponseBody
	public ResponseEntity<Object> resolveException(ResourceNotFoundException exception) {

		return new ResponseEntity<>(exception.getMessage(), HttpStatus.NOT_FOUND);
	}


	@ExceptionHandler(WrongInputException.class)
	@ResponseBody
	public ResponseEntity<Object> resolveException(WrongInputException exception) {

		return new ResponseEntity<>(exception.getMessage(), HttpStatus.BAD_REQUEST);
	}


	@ExceptionHandler(ApplicationException.class)
	@ResponseBody
	public ResponseEntity<Object> resolveException(ApplicationException exception) {

		log.error(exception.getMessage(), exception);
		return new ResponseEntity<>(exception.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
	}

}
