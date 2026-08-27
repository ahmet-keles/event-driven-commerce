package com.ahmetkeles.paymentservice.payment;

import com.ahmetkeles.paymentservice.gateway.PaymentGateway;
import com.ahmetkeles.paymentservice.gateway.PaymentGatewayRequest;
import com.ahmetkeles.paymentservice.gateway.PaymentGatewayResult;
import com.ahmetkeles.paymentservice.outbox.OutboxEvent;
import com.ahmetkeles.paymentservice.outbox.OutboxEventRepository;
import com.ahmetkeles.paymentservice.outbox.PaymentCompletedEvent;
import com.ahmetkeles.paymentservice.outbox.PaymentFailedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Charges one payment per confirmed order, exactly once from the domain's
 * point of view, and records the terminal outcome immutably.
 *
 * <h2>Idempotency, in three layers</h2>
 * <ul>
 *   <li>{@code processed_events} dedups redeliveries of the same eventId
 *       inside the same transaction as the state change;</li>
 *   <li>the {@code payments.order_id} unique constraint plus the
 *       {@code existsByOrderId} guard dedup a re-emitted ORDER_CONFIRMED that
 *       carries a fresh eventId — the second delivery records its eventId as
 *       processed and changes nothing else;</li>
 *   <li>the gateway idempotency key is the orderId, so a crash after the
 *       charge but before the commit replays the charge under the same key on
 *       redelivery and the provider returns the original outcome instead of
 *       charging twice.</li>
 * </ul>
 *
 * <h2>Immutability</h2>
 * A payment row is written once, already terminal ({@code COMPLETED} or
 * {@code FAILED}), and no code path updates it. A decline is a modeled
 * business outcome — it produces PAYMENT_FAILED through the outbox, never an
 * exception, so it is never retried and never dead-lettered.
 */
@Service
public class PaymentService {

    private static final Logger log =
            LoggerFactory.getLogger(PaymentService.class);

    private static final String AGGREGATE_TYPE = "Order";
    private static final String PAYMENT_COMPLETED = "PAYMENT_COMPLETED";
    private static final String PAYMENT_FAILED = "PAYMENT_FAILED";

    private final PaymentRepository paymentRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final PaymentGateway paymentGateway;
    private final ObjectMapper objectMapper;

    public PaymentService(
            PaymentRepository paymentRepository,
            ProcessedEventRepository processedEventRepository,
            OutboxEventRepository outboxEventRepository,
            PaymentGateway paymentGateway,
            ObjectMapper objectMapper
    ) {
        this.paymentRepository = paymentRepository;
        this.processedEventRepository = processedEventRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.paymentGateway = paymentGateway;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void processOrderConfirmed(
            UUID eventId,
            String eventType,
            UUID orderId,
            UUID customerId,
            BigDecimal totalAmount,
            String currency
    ) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }

        // A re-emitted ORDER_CONFIRMED arrives with a new eventId but the
        // same order. The payment's terminal outcome is immutable, so the
        // duplicate only records its eventId and leaves everything else —
        // payment row, outbox — untouched.
        if (paymentRepository.existsByOrderId(orderId)) {
            processedEventRepository.save(
                    new ProcessedEvent(eventId, eventType)
            );

            log.info(
                    "Ignoring duplicate ORDER_CONFIRMED {} for already-paid order {}",
                    eventId,
                    orderId
            );

            return;
        }

        // The idempotency key is the orderId — the business operation — so a
        // redelivery after a crash between charge and commit replays the same
        // key and the provider returns the original outcome.
        PaymentGatewayResult result = paymentGateway.charge(
                new PaymentGatewayRequest(
                        orderId.toString(),
                        totalAmount,
                        currency
                )
        );

        Payment payment = result.approved()
                ? Payment.completed(
                        orderId,
                        customerId,
                        totalAmount,
                        currency,
                        result.gatewayReference()
                )
                : Payment.failed(
                        orderId,
                        customerId,
                        totalAmount,
                        currency,
                        result.gatewayReference(),
                        result.declineReason()
                );

        paymentRepository.save(payment);

        processedEventRepository.save(new ProcessedEvent(eventId, eventType));

        outboxEventRepository.save(new OutboxEvent(
                AGGREGATE_TYPE,
                orderId,
                result.approved() ? PAYMENT_COMPLETED : PAYMENT_FAILED,
                serialize(result.approved()
                        ? new PaymentCompletedEvent(
                                orderId,
                                payment.getId(),
                                totalAmount,
                                currency
                        )
                        : new PaymentFailedEvent(
                                orderId,
                                payment.getId(),
                                totalAmount,
                                currency,
                                payment.getFailureReason()
                        )
                )
        ));
    }

    private String serialize(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception exception) {
            throw new IllegalStateException(
                    "Failed to serialize payment event",
                    exception
            );
        }
    }
}
