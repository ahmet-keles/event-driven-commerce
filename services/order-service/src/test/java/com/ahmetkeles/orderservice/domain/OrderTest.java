package com.ahmetkeles.orderservice.domain;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OrderTest {

    @Test
    void orderRequiresCustomerId() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new Order(null, "USD")
        );
    }

    @Test
    void orderRequiresCurrency() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new Order(UUID.randomUUID(), null)
        );
    }

    @Test
    void orderRequiresNonBlankCurrency() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new Order(UUID.randomUUID(), "  ")
        );
    }

    @Test
    void newOrderStartsWithZeroTotal() {
        Order order = new Order(UUID.randomUUID(), "USD");

        assertEquals(BigDecimal.ZERO, order.getTotalAmount());
    }

    @Test
    void addItemAddsItem() {
        Order order = new Order(UUID.randomUUID(), "USD");

        order.addItem(UUID.randomUUID(), 2, new BigDecimal("15.00"));

        assertEquals(1, order.getItems().size());
    }

    @Test
    void addItemUpdatesTotal() {
        Order order = new Order(UUID.randomUUID(), "USD");

        order.addItem(UUID.randomUUID(), 2, new BigDecimal("15.00"));

        assertEquals(new BigDecimal("30.00"), order.getTotalAmount());
    }

    @Test
    void multipleItemsAccumulateTotal() {
        Order order = new Order(UUID.randomUUID(), "USD");

        order.addItem(UUID.randomUUID(), 2, new BigDecimal("15.00"));
        order.addItem(UUID.randomUUID(), 3, new BigDecimal("4.50"));

        assertEquals(new BigDecimal("43.50"), order.getTotalAmount());
    }

    @Test
    void singleItemOrderIsConfirmedOnceThatItemIsReserved() {
        Order order = new Order(UUID.randomUUID(), "USD");
        OrderItem item = order.addItem(
                UUID.randomUUID(), 2, new BigDecimal("15.00"));
        order.submit();

        order.markItemReserved(item.getId());

        assertEquals(OrderStatus.CONFIRMED, order.getStatus());
    }

    @Test
    void twoItemOrderStaysPendingAfterFirstReservation() {
        Order order = new Order(UUID.randomUUID(), "USD");
        OrderItem first = order.addItem(
                UUID.randomUUID(), 1, new BigDecimal("10.00"));
        order.addItem(UUID.randomUUID(), 1, new BigDecimal("20.00"));
        order.submit();

        order.markItemReserved(first.getId());

        assertEquals(OrderStatus.PENDING, order.getStatus());
    }

    @Test
    void twoItemOrderIsConfirmedAfterSecondReservation() {
        Order order = new Order(UUID.randomUUID(), "USD");
        OrderItem first = order.addItem(
                UUID.randomUUID(), 1, new BigDecimal("10.00"));
        OrderItem second = order.addItem(
                UUID.randomUUID(), 1, new BigDecimal("20.00"));
        order.submit();

        order.markItemReserved(first.getId());
        order.markItemReserved(second.getId());

        assertEquals(OrderStatus.CONFIRMED, order.getStatus());
    }

    @Test
    void duplicateReservationForSameItemDoesNotConfirmOrder() {
        Order order = new Order(UUID.randomUUID(), "USD");
        OrderItem first = order.addItem(
                UUID.randomUUID(), 1, new BigDecimal("10.00"));
        order.addItem(UUID.randomUUID(), 1, new BigDecimal("20.00"));
        order.submit();

        order.markItemReserved(first.getId());

        Instant updatedAtAfterFirstReservation = order.getUpdatedAt();

        order.markItemReserved(first.getId());
        order.markItemReserved(first.getId());

        assertEquals(OrderStatus.PENDING, order.getStatus());
        assertEquals(updatedAtAfterFirstReservation, order.getUpdatedAt());
    }

    @Test
    void duplicateReservationForSameProductDoesNotConfirmOrder() {
        UUID sharedProductId = UUID.randomUUID();
        Order order = new Order(UUID.randomUUID(), "USD");
        OrderItem first = order.addItem(
                sharedProductId, 1, new BigDecimal("10.00"));
        order.addItem(sharedProductId, 1, new BigDecimal("10.00"));
        order.submit();

        order.markItemReserved(first.getId());
        order.markItemReserved(first.getId());

        assertEquals(OrderStatus.PENDING, order.getStatus());
    }

    @Test
    void reservationForUnknownItemLeavesOrderUntouched() {
        Order order = new Order(UUID.randomUUID(), "USD");
        order.addItem(UUID.randomUUID(), 1, new BigDecimal("10.00"));

        Instant updatedAtBefore = order.getUpdatedAt();

        order.markItemReserved(UUID.randomUUID());

        assertEquals(OrderStatus.PENDING, order.getStatus());
        assertEquals(updatedAtBefore, order.getUpdatedAt());
    }

    @Test
    void reservationForNullItemIdLeavesOrderUntouched() {
        Order order = new Order(UUID.randomUUID(), "USD");
        order.addItem(UUID.randomUUID(), 1, new BigDecimal("10.00"));

        Instant updatedAtBefore = order.getUpdatedAt();

        order.markItemReserved(null);

        assertEquals(OrderStatus.PENDING, order.getStatus());
        assertEquals(updatedAtBefore, order.getUpdatedAt());
    }

    @Test
    void orderWithoutItemsIsNotConfirmed() {
        Order order = new Order(UUID.randomUUID(), "USD");

        order.markItemReserved(UUID.randomUUID());

        assertEquals(OrderStatus.PENDING, order.getStatus());
    }

    @Test
    void reservingAllItemsTwiceKeepsOrderConfirmed() {
        Order order = new Order(UUID.randomUUID(), "USD");
        OrderItem item = order.addItem(
                UUID.randomUUID(), 1, new BigDecimal("10.00"));
        order.submit();

        order.markItemReserved(item.getId());
        order.markItemReserved(item.getId());

        assertEquals(OrderStatus.CONFIRMED, order.getStatus());
    }

    @Test
    void pendingOrderCanBeCancelled() {
        Order order = new Order(UUID.randomUUID(), "USD");

        order.cancel();

        assertEquals(OrderStatus.CANCELLED, order.getStatus());
    }

    @Test
    void cancelReportsWhetherTheTransitionHappened() {
        Order order = new Order(UUID.randomUUID(), "USD");

        assertTrue(order.cancel(), "first cancel performs the transition");
        assertFalse(order.cancel(), "second cancel is a true no-op");

        Order confirmed = new Order(UUID.randomUUID(), "USD");
        OrderItem item = confirmed.addItem(
                UUID.randomUUID(), 1, new BigDecimal("10.00"));
        confirmed.submit();
        confirmed.markItemReserved(item.getId());

        assertFalse(confirmed.cancel(),
                "a confirmed order reports no transition");
    }

    @Test
    void cancellingAlreadyCancelledOrderIsIdempotent() {
        Order order = new Order(UUID.randomUUID(), "USD");

        order.cancel();
        order.cancel();

        assertEquals(OrderStatus.CANCELLED, order.getStatus());
    }

    @Test
    void confirmedOrderIsNotCancelledByLateFailure() {
        Order order = new Order(UUID.randomUUID(), "USD");
        OrderItem item = order.addItem(
                UUID.randomUUID(), 1, new BigDecimal("10.00"));
        order.submit();

        order.markItemReserved(item.getId());
        order.cancel();

        assertEquals(OrderStatus.CONFIRMED, order.getStatus());
    }

    @Test
    void cancelledOrderIsNotConfirmedByLateReservation() {
        Order order = new Order(UUID.randomUUID(), "USD");
        OrderItem item = order.addItem(
                UUID.randomUUID(), 1, new BigDecimal("10.00"));

        order.cancel();
        order.markItemReserved(item.getId());

        assertEquals(OrderStatus.CANCELLED, order.getStatus());
    }

    @Test
    void partiallyReservedOrderIsCancelledByFailure() {
        Order order = new Order(UUID.randomUUID(), "USD");
        OrderItem first = order.addItem(
                UUID.randomUUID(), 1, new BigDecimal("10.00"));
        order.addItem(UUID.randomUUID(), 1, new BigDecimal("20.00"));

        order.markItemReserved(first.getId());
        order.cancel();

        assertEquals(OrderStatus.CANCELLED, order.getStatus());
    }

    @Test
    void cancelledOrderDoesNotRecordFurtherItemReservations() {
        Order order = new Order(UUID.randomUUID(), "USD");
        OrderItem first = order.addItem(
                UUID.randomUUID(), 1, new BigDecimal("10.00"));
        OrderItem second = order.addItem(
                UUID.randomUUID(), 1, new BigDecimal("20.00"));

        order.markItemReserved(first.getId());
        order.cancel();

        Instant updatedAtAfterCancel = order.getUpdatedAt();

        order.markItemReserved(second.getId());

        assertEquals(OrderStatus.CANCELLED, order.getStatus());
        assertEquals(updatedAtAfterCancel, order.getUpdatedAt());
        assertFalse(order.getItems().stream()
                .filter(item -> item.getId().equals(second.getId()))
                .findFirst()
                .orElseThrow()
                .isReserved());
    }

    @Test
    void addItemOnConfirmedOrderIsRejectedWithoutMutation() {
        Order order = new Order(UUID.randomUUID(), "USD");
        OrderItem item = order.addItem(
                UUID.randomUUID(), 2, new BigDecimal("15.00"));
        order.submit();
        order.markItemReserved(item.getId());

        BigDecimal totalBefore = order.getTotalAmount();
        Instant updatedAtBefore = order.getUpdatedAt();

        assertThrows(
                OrderNotModifiableException.class,
                () -> order.addItem(
                        UUID.randomUUID(), 1, new BigDecimal("5.00"))
        );

        assertEquals(OrderStatus.CONFIRMED, order.getStatus());
        assertEquals(1, order.getItems().size());
        assertEquals(totalBefore, order.getTotalAmount());
        assertEquals(updatedAtBefore, order.getUpdatedAt());
    }

    @Test
    void addItemOnCancelledOrderIsRejectedWithoutMutation() {
        Order order = new Order(UUID.randomUUID(), "USD");
        order.addItem(UUID.randomUUID(), 2, new BigDecimal("15.00"));
        order.cancel();

        BigDecimal totalBefore = order.getTotalAmount();
        Instant updatedAtBefore = order.getUpdatedAt();

        assertThrows(
                OrderNotModifiableException.class,
                () -> order.addItem(
                        UUID.randomUUID(), 1, new BigDecimal("5.00"))
        );

        assertEquals(OrderStatus.CANCELLED, order.getStatus());
        assertEquals(1, order.getItems().size());
        assertEquals(totalBefore, order.getTotalAmount());
        assertEquals(updatedAtBefore, order.getUpdatedAt());
    }

    @Test
    void addItemOnTerminalOrderRejectsEvenInvalidArguments() {
        Order order = new Order(UUID.randomUUID(), "USD");
        order.cancel();

        assertThrows(
                OrderNotModifiableException.class,
                () -> order.addItem(UUID.randomUUID(), 0, null)
        );
    }

    @Test
    void newOrderIsUnsubmitted() {
        Order order = new Order(UUID.randomUUID(), "USD");

        assertFalse(order.isSubmitted());
        assertEquals(OrderStatus.PENDING, order.getStatus());
    }

    @Test
    void cannotSubmitEmptyOrder() {
        Order order = new Order(UUID.randomUUID(), "USD");

        Instant updatedAtBefore = order.getUpdatedAt();

        assertThrows(
                EmptyOrderSubmissionException.class,
                order::submit
        );

        assertFalse(order.isSubmitted());
        assertEquals(OrderStatus.PENDING, order.getStatus());
        assertEquals(updatedAtBefore, order.getUpdatedAt());
    }

    @Test
    void submittedOrderRejectsAddItemWithoutMutation() {
        Order order = new Order(UUID.randomUUID(), "USD");
        order.addItem(UUID.randomUUID(), 2, new BigDecimal("15.00"));
        order.submit();

        BigDecimal totalBefore = order.getTotalAmount();
        Instant updatedAtBefore = order.getUpdatedAt();

        assertThrows(
                OrderNotModifiableException.class,
                () -> order.addItem(
                        UUID.randomUUID(), 1, new BigDecimal("5.00"))
        );

        assertEquals(OrderStatus.PENDING, order.getStatus());
        assertEquals(1, order.getItems().size());
        assertEquals(totalBefore, order.getTotalAmount());
        assertEquals(updatedAtBefore, order.getUpdatedAt());
    }

    @Test
    void allItemsReservedBeforeSubmitDoesNotConfirm() {
        Order order = new Order(UUID.randomUUID(), "USD");
        OrderItem item = order.addItem(
                UUID.randomUUID(), 2, new BigDecimal("15.00"));

        order.markItemReserved(item.getId());

        assertEquals(OrderStatus.PENDING, order.getStatus(),
                "an unsubmitted order must never confirm, however fast "
                        + "reservations arrive");
        assertFalse(order.isSubmitted());
    }

    @Test
    void submittingFullyReservedOrderConfirmsImmediately() {
        Order order = new Order(UUID.randomUUID(), "USD");
        OrderItem item = order.addItem(
                UUID.randomUUID(), 2, new BigDecimal("15.00"));
        order.markItemReserved(item.getId());

        assertTrue(order.submit(),
                "first submit performs the transition");

        assertTrue(order.isSubmitted());
        assertEquals(OrderStatus.CONFIRMED, order.getStatus(),
                "reservations that finished before submission must confirm "
                        + "in the submitting call itself");
    }

    @Test
    void submitBeforeReservationsWaitsForLastReservation() {
        Order order = new Order(UUID.randomUUID(), "USD");
        OrderItem first = order.addItem(
                UUID.randomUUID(), 1, new BigDecimal("10.00"));
        OrderItem second = order.addItem(
                UUID.randomUUID(), 1, new BigDecimal("20.00"));

        order.submit();
        assertEquals(OrderStatus.PENDING, order.getStatus());

        order.markItemReserved(first.getId());
        assertEquals(OrderStatus.PENDING, order.getStatus());

        order.markItemReserved(second.getId());
        assertEquals(OrderStatus.CONFIRMED, order.getStatus());
    }

    @Test
    void duplicateSubmitDoesNotProduceAnotherTransition() {
        Order order = new Order(UUID.randomUUID(), "USD");
        order.addItem(UUID.randomUUID(), 1, new BigDecimal("10.00"));

        assertTrue(order.submit());

        Instant updatedAtAfterSubmit = order.getUpdatedAt();

        assertFalse(order.submit(), "second submit is a true no-op");
        assertFalse(order.submit(), "third submit is a true no-op");

        assertEquals(OrderStatus.PENDING, order.getStatus());
        assertTrue(order.isSubmitted());
        assertEquals(updatedAtAfterSubmit, order.getUpdatedAt());
    }

    @Test
    void duplicateSubmitAfterConfirmationIsNoOp() {
        Order order = new Order(UUID.randomUUID(), "USD");
        OrderItem item = order.addItem(
                UUID.randomUUID(), 1, new BigDecimal("10.00"));
        order.markItemReserved(item.getId());
        order.submit();

        Instant updatedAtAfterConfirm = order.getUpdatedAt();

        assertFalse(order.submit());

        assertEquals(OrderStatus.CONFIRMED, order.getStatus());
        assertEquals(updatedAtAfterConfirm, order.getUpdatedAt());
    }

    @Test
    void submitOnCancelledOrderThrows() {
        Order order = new Order(UUID.randomUUID(), "USD");
        order.addItem(UUID.randomUUID(), 1, new BigDecimal("10.00"));
        order.cancel();

        assertThrows(
                OrderNotModifiableException.class,
                order::submit
        );

        assertFalse(order.isSubmitted());
        assertEquals(OrderStatus.CANCELLED, order.getStatus());
    }

    @Test
    void submittedPendingOrderCanStillBeCancelled() {
        Order order = new Order(UUID.randomUUID(), "USD");
        order.addItem(UUID.randomUUID(), 1, new BigDecimal("10.00"));
        order.submit();

        assertTrue(order.cancel(),
                "a reservation failure must still cancel a submitted, "
                        + "not-yet-confirmed order");
        assertEquals(OrderStatus.CANCELLED, order.getStatus());
    }

    @Test
    void itemsCannotBeModifiedByCallers() {
        Order order = new Order(UUID.randomUUID(), "USD");

        assertThrows(
                UnsupportedOperationException.class,
                () -> order.getItems().add(new OrderItem(
                        UUID.randomUUID(), 1, BigDecimal.ONE, order))
        );
    }

    @Test
    void markItemReservedReportsOnlyTheConfirmingCall() {
        Order order = new Order(UUID.randomUUID(), "USD");
        OrderItem first = order.addItem(
                UUID.randomUUID(), 1, new BigDecimal("10.00"));
        OrderItem second = order.addItem(
                UUID.randomUUID(), 1, new BigDecimal("20.00"));
        order.submit();

        assertFalse(order.markItemReserved(first.getId()),
                "a non-final reservation must not report confirmation");
        assertFalse(order.markItemReserved(first.getId()),
                "a duplicate must not report confirmation");
        assertFalse(order.markItemReserved(UUID.randomUUID()),
                "an unknown item must not report confirmation");
        assertTrue(order.markItemReserved(second.getId()),
                "only the call that confirms the order reports true");
        assertFalse(order.markItemReserved(second.getId()),
                "a redelivery after confirmation reports false");
    }

    @Test
    void reservationsOnAnUnsubmittedOrderNeverReportConfirmation() {
        Order order = new Order(UUID.randomUUID(), "USD");
        OrderItem item = order.addItem(
                UUID.randomUUID(), 1, new BigDecimal("10.00"));

        assertFalse(order.markItemReserved(item.getId()),
                "every item is reserved, but assembly is not finished — "
                        + "the reservation must not confirm or start payment");
        assertEquals(OrderStatus.PENDING, order.getStatus());
        assertEquals(PaymentStatus.NOT_STARTED, order.getPaymentStatus());
    }

    @Test
    void confirmationViaFinalReservationStartsThePaymentLeg() {
        Order order = new Order(UUID.randomUUID(), "USD");
        OrderItem item = order.addItem(
                UUID.randomUUID(), 1, new BigDecimal("10.00"));
        order.submit();

        assertEquals(PaymentStatus.NOT_STARTED, order.getPaymentStatus());

        assertTrue(order.markItemReserved(item.getId()));

        assertEquals(OrderStatus.CONFIRMED, order.getStatus());
        assertEquals(PaymentStatus.PENDING, order.getPaymentStatus());
    }

    @Test
    void confirmationViaSubmitStartsThePaymentLeg() {
        Order order = new Order(UUID.randomUUID(), "USD");
        OrderItem item = order.addItem(
                UUID.randomUUID(), 1, new BigDecimal("10.00"));
        order.markItemReserved(item.getId());

        assertEquals(PaymentStatus.NOT_STARTED, order.getPaymentStatus(),
                "reserved but unsubmitted: payment must not have started");

        assertTrue(order.submit(),
                "the submit over fully-reserved items performs the "
                        + "confirmation");

        assertEquals(OrderStatus.CONFIRMED, order.getStatus());
        assertEquals(PaymentStatus.PENDING, order.getPaymentStatus());
    }

    @Test
    void firstTerminalPaymentOutcomeWins() {
        Order completed = confirmedOrder();
        assertTrue(completed.completePayment());
        assertFalse(completed.failPayment(),
                "COMPLETED must never become FAILED");
        assertFalse(completed.completePayment(),
                "duplicate completion is a no-op");
        assertEquals(PaymentStatus.COMPLETED, completed.getPaymentStatus());

        Order failed = confirmedOrder();
        assertTrue(failed.failPayment());
        assertFalse(failed.completePayment(),
                "FAILED must never resurrect to COMPLETED");
        assertFalse(failed.failPayment(),
                "duplicate failure is a no-op");
        assertEquals(PaymentStatus.FAILED, failed.getPaymentStatus());
    }

    @Test
    void paymentOutcomesRequireAStartedPayment() {
        Order order = new Order(UUID.randomUUID(), "USD");

        assertFalse(order.completePayment(),
                "a payment that never started cannot complete");
        assertFalse(order.failPayment(),
                "a payment that never started cannot fail");
        assertEquals(PaymentStatus.NOT_STARTED, order.getPaymentStatus());
    }

    @Test
    void paymentFailureCancellationOnlyCancelsConfirmedOrders() {
        Order confirmed = confirmedOrder();
        assertTrue(confirmed.cancelForFailedPayment());
        assertEquals(OrderStatus.CANCELLED, confirmed.getStatus());
        assertFalse(confirmed.cancelForFailedPayment(),
                "a cancelled order does not resurrect or re-cancel");

        Order pending = new Order(UUID.randomUUID(), "USD");
        assertFalse(pending.cancelForFailedPayment(),
                "the payment-failure transition never touches a PENDING order");
        assertEquals(OrderStatus.PENDING, pending.getStatus());
    }

    @Test
    void genericCancelStillRefusesConfirmedOrders() {
        Order confirmed = confirmedOrder();

        assertFalse(confirmed.cancel(),
                "generic cancellation semantics must remain unchanged");
        assertEquals(OrderStatus.CONFIRMED, confirmed.getStatus());
    }

    private static Order confirmedOrder() {
        Order order = new Order(UUID.randomUUID(), "USD");
        OrderItem item = order.addItem(
                UUID.randomUUID(), 1, new BigDecimal("10.00"));
        order.submit();
        order.markItemReserved(item.getId());
        return order;
    }
}
