package com.ahmetkeles.orderservice.api;

import com.ahmetkeles.orderservice.domain.EmptyOrderSubmissionException;
import com.ahmetkeles.orderservice.domain.OrderNotModifiableException;
import com.ahmetkeles.orderservice.domain.OrderStatus;
import com.ahmetkeles.orderservice.service.OrderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.math.BigDecimal;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.containsString;

class RestExceptionHandlerTest {

    private static final String INTERNAL_DETAIL =
            "row was updated or deleted by another transaction";

    private OrderService orderService;
    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        orderService = mock(OrderService.class);
        mockMvc = MockMvcBuilders
                .standaloneSetup(new OrderController(orderService))
                .setControllerAdvice(new RestExceptionHandler())
                .build();
    }

    @Test
    void optimisticLockingConflictMapsToConflictResponse() throws Exception {
        when(orderService.addItem(
                any(UUID.class), any(UUID.class), anyInt(), any(BigDecimal.class)))
                .thenThrow(new OptimisticLockingFailureException(INTERNAL_DETAIL));

        mockMvc.perform(post("/api/orders/{orderId}/items", UUID.randomUUID())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(itemRequest()))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status").value(409))
                .andExpect(jsonPath("$.error").value("concurrent_modification"))
                .andExpect(content().string(not(containsString(INTERNAL_DETAIL))));
    }

    @Test
    void orderNotModifiableMapsToConflictResponse() throws Exception {
        UUID orderId = UUID.randomUUID();

        when(orderService.addItem(
                any(UUID.class), any(UUID.class), anyInt(), any(BigDecimal.class)))
                .thenThrow(new OrderNotModifiableException(
                        orderId, OrderStatus.CANCELLED));

        mockMvc.perform(post("/api/orders/{orderId}/items", orderId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(itemRequest()))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status").value(409))
                .andExpect(jsonPath("$.error").value("order_not_modifiable"))
                .andExpect(content().string(not(containsString(orderId.toString()))));
    }

    @Test
    void optimisticLockingConflictOnSubmitMapsToConflictResponse() throws Exception {
        when(orderService.submitOrder(any(UUID.class)))
                .thenThrow(new OptimisticLockingFailureException(INTERNAL_DETAIL));

        mockMvc.perform(post("/api/orders/{orderId}/submit", UUID.randomUUID()))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status").value(409))
                .andExpect(jsonPath("$.error").value("concurrent_modification"))
                .andExpect(content().string(not(containsString(INTERNAL_DETAIL))));
    }

    @Test
    void emptyOrderSubmissionMapsToConflictResponse() throws Exception {
        UUID orderId = UUID.randomUUID();

        when(orderService.submitOrder(any(UUID.class)))
                .thenThrow(new EmptyOrderSubmissionException(orderId));

        mockMvc.perform(post("/api/orders/{orderId}/submit", orderId))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status").value(409))
                .andExpect(jsonPath("$.error").value("order_empty"))
                .andExpect(content().string(not(containsString(orderId.toString()))));
    }

    private String itemRequest() {
        return "{\"productId\":\"%s\",\"quantity\":1,\"unitPrice\":10.00}"
                .formatted(UUID.randomUUID());
    }
}
