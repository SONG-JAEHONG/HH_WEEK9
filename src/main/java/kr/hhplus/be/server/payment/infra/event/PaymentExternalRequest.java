package kr.hhplus.be.server.payment.infra.event;

import java.time.Instant;

public record PaymentExternalRequest(
        Long reservationId,
        Long userId,
        Long amount,
        String status,
        Instant requestedAt
) { }
