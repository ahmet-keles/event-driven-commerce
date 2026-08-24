package com.ahmetkeles.orderservice.api;

public record ApiError(int status, String error, String message) {
}
