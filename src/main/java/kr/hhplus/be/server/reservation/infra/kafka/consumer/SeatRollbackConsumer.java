package kr.hhplus.be.server.reservation.infra.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import kr.hhplus.be.server.concert.domain.Seat;
import kr.hhplus.be.server.concert.port.out.SeatRepository;
import kr.hhplus.be.server.reservation.domain.ReservationOutbox;
import kr.hhplus.be.server.reservation.domain.ReservationOutboxStatus;
import kr.hhplus.be.server.reservation.infra.kafka.dto.RollbackCommandMessage;
import kr.hhplus.be.server.reservation.port.out.ReservationOutboxRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.kafka", name = "enabled", havingValue = "true")
public class SeatRollbackConsumer {

    private final ObjectMapper objectMapper;
    private final ReservationOutboxRepository outboxRepository;
    private final SeatRepository seatRepository;

    @KafkaListener(topics = "reservation_rollback", groupId = "reservation-service")
    @Transactional
    public void onRollback(String payload) throws Exception {
        RollbackCommandMessage cmd = objectMapper.readValue(payload, RollbackCommandMessage.class);

        ReservationOutbox outbox = outboxRepository.findById(cmd.outboxId())
                .orElseThrow(() -> new IllegalStateException("Outbox not found: " + cmd.outboxId()));

        outbox.setStatus(ReservationOutboxStatus.ROLLBACK_RECEIPT);
        outboxRepository.save(outbox);

        Seat seat = seatRepository.findSeatByIdOrThrow(cmd.seatId());
        seat.rollbackHold();
        seatRepository.save(seat);

        outbox.setStatus(ReservationOutboxStatus.ROLLBACK_DONE);
        outboxRepository.save(outbox);
    }
}
