package kr.hhplus.be.server.payment.infra.event;

public record PaymentCreatedEvent(
        Long reservationId,
        Long userId,
        Long amount
) { }
