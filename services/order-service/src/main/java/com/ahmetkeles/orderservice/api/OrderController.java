package com.ahmetkeles.orderservice.api;

import com.ahmetkeles.orderservice.domain.Order;
import com.ahmetkeles.orderservice.service.OrderService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<OrderResponse> createOrder(@Valid @RequestBody CreateOrderRequest request) {
        Order order = orderService.createOrder(request.customerId(), request.currency());
        return ResponseEntity.status(HttpStatus.CREATED).body(OrderResponse.from(order));
    }

    @GetMapping("/{orderId}")
    public OrderResponse getOrder(@PathVariable UUID orderId) {
        return OrderResponse.from(orderService.getOrder(orderId));
    }

    @PostMapping("/{orderId}/submit")
    public OrderResponse submitOrder(@PathVariable UUID orderId) {
        return OrderResponse.from(orderService.submitOrder(orderId));
    }

    @PostMapping("/{orderId}/items")
    public OrderResponse addItem(
            @PathVariable UUID orderId,
            @Valid @RequestBody AddOrderItemRequest request
    ) {
        return OrderResponse.from(orderService.addItem(
                orderId,
                request.productId(),
                request.quantity(),
                request.unitPrice()
        ));
    }
}
