package com.ahmetkeles.orderservice.api;

import com.ahmetkeles.orderservice.domain.OrderNotModifiableException;
import com.ahmetkeles.orderservice.service.OrderNotFoundException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@RestControllerAdvice
public class RestExceptionHandler {

    @ExceptionHandler(OrderNotFoundException.class)
    public ResponseEntity<ApiError> handleOrderNotFound() {
        return error(HttpStatus.NOT_FOUND, "not_found", "Order not found");
    }

    @ExceptionHandler(OrderNotModifiableException.class)
    public ResponseEntity<ApiError> handleOrderNotModifiable() {
        return error(
                HttpStatus.CONFLICT,
                "order_not_modifiable",
                "Order can no longer be modified"
        );
    }

    @ExceptionHandler(OptimisticLockingFailureException.class)
    public ResponseEntity<ApiError> handleOptimisticLockingFailure() {
        return error(
                HttpStatus.CONFLICT,
                "concurrent_modification",
                "The order was modified concurrently; retry the request"
        );
    }

    @ExceptionHandler({MethodArgumentNotValidException.class, MethodArgumentTypeMismatchException.class})
    public ResponseEntity<ApiError> handleValidationFailure() {
        return error(HttpStatus.BAD_REQUEST, "validation_error", "Request validation failed");
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ApiError> handleIllegalArgument() {
        return error(HttpStatus.BAD_REQUEST, "invalid_argument", "Invalid request");
    }

    private ResponseEntity<ApiError> error(HttpStatus status, String error, String message) {
        return ResponseEntity.status(status).body(new ApiError(status.value(), error, message));
    }
}
