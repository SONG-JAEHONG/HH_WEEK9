package kr.hhplus.be.server.reservation.infra.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;

import kr.hhplus.be.server.concert.domain.ConcertDate;
import kr.hhplus.be.server.concert.domain.Seat;
import kr.hhplus.be.server.concert.port.out.ConcertRepository;
import kr.hhplus.be.server.concert.port.out.SeatRepository;
import kr.hhplus.be.server.reservation.domain.Reservation;
import kr.hhplus.be.server.reservation.domain.ReservationOutbox;
import kr.hhplus.be.server.reservation.domain.ReservationOutboxStatus;
import kr.hhplus.be.server.reservation.infra.kafka.dto.ReservationCommandMessage;
import kr.hhplus.be.server.reservation.infra.kafka.dto.RollbackCommandMessage;
import kr.hhplus.be.server.reservation.port.out.ReservationOutboxRepository;
import kr.hhplus.be.server.reservation.port.out.ReservationRepository;
import kr.hhplus.be.server.user.domain.User;
import kr.hhplus.be.server.user.port.out.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.kafka", name = "enabled", havingValue = "true")
public class ReservationCommandConsumer {

    private final ObjectMapper objectMapper;
    private final ReservationOutboxRepository outboxRepository;

    private final UserRepository userRepository;
    private final ConcertRepository concertRepository;
    private final SeatRepository seatRepository;
    private final ReservationRepository reservationRepository;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private static final String ROLLBACK_TOPIC = "reservation_rollback";

    @KafkaListener(topics = "reservation", groupId = "reservation-service")
    @Transactional
    public void onReservation(String payload) throws Exception {
        ReservationCommandMessage cmd = objectMapper.readValue(payload, ReservationCommandMessage.class);

        ReservationOutbox outbox = outboxRepository.findById(cmd.outboxId())
                .orElseThrow(() -> new IllegalStateException("Outbox not found: " + cmd.outboxId()));


        outbox.setStatus(ReservationOutboxStatus.RECEIPT);
        outboxRepository.save(outbox);

        try {
            User user = userRepository.findUserByIdOrThrow(cmd.userId());
            ConcertDate concertDate = concertRepository.findConcertDateByIdOrThrow(cmd.concertDateId());
            Seat seat = seatRepository.findSeatByIdOrThrow(cmd.seatId());

            Reservation reservation = Reservation.holding(user, concertDate, seat);
            reservationRepository.save(reservation);


            outbox.setReservationId(reservation.getId());
            outbox.setStatus(ReservationOutboxStatus.SUCCESS);
            outboxRepository.save(outbox);

        } catch (Exception e) {

            RollbackCommandMessage rb = new RollbackCommandMessage(cmd.outboxId(), cmd.seatId());
            String rbJson = objectMapper.writeValueAsString(rb);
            kafkaTemplate.send(ROLLBACK_TOPIC, rbJson);
            throw e;
        }
    }
}