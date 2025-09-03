package kr.hhplus.be.server.reservation.infra.kafka.dto;

public record RollbackCommandMessage(
        Long outboxId,
        Long seatId
) {
}
