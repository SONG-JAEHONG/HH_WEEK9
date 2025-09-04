package kr.hhplus.be.server.reservation.infra.kafka.dto;

public record ReservationCommandMessage(
        Long outboxId,
        Long userId,
        Long concertDateId,
        Long seatId
) {
}
