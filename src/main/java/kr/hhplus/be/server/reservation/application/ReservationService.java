package kr.hhplus.be.server.reservation.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import kr.hhplus.be.server.concert.domain.ConcertDate;
import kr.hhplus.be.server.concert.domain.Seat;
import kr.hhplus.be.server.concert.port.out.ConcertRepository;
import kr.hhplus.be.server.concert.port.out.SeatRepository;
import kr.hhplus.be.server.payment.port.out.PaymentRepository;
import kr.hhplus.be.server.reservation.domain.Reservation;
import kr.hhplus.be.server.reservation.domain.ReservationOutbox;
import kr.hhplus.be.server.reservation.domain.ReservationOutboxStatus;
import kr.hhplus.be.server.reservation.infra.kafka.dto.ReservationCommandMessage;
import kr.hhplus.be.server.reservation.infra.web.dto.ReservationRequest;
import kr.hhplus.be.server.reservation.infra.web.dto.ReservationResponse;
import kr.hhplus.be.server.reservation.port.in.ReservationUseCase;
import kr.hhplus.be.server.reservation.port.out.ReservationOutboxRepository;
import kr.hhplus.be.server.reservation.port.out.ReservationRepository;
import kr.hhplus.be.server.user.domain.User;
import kr.hhplus.be.server.user.port.out.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class ReservationService implements ReservationUseCase {

    private final UserRepository userRepository;
    private final ReservationRepository reservationRepository;
    private final ConcertRepository concertRepository;
    private final SeatRepository seatRepository;
    private final SeatHoldService seatHoldService;
    private final SeatHoldOrchestrator seatHoldOrchestrator;


    private final ReservationOutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    @Override
    public ReservationResponse reserve(ReservationRequest reservationRequest, Long userId) {

        User user = userRepository.findUserByIdOrThrow(userId);

        Seat seat = seatHoldOrchestrator.holdSeatWithLock(reservationRequest.seatId());

        ConcertDate concertDate = concertRepository.findConcertDateByIdOrThrow(reservationRequest.concertDateId()) ;

        ReservationOutbox outbox = ReservationOutbox.builder()
                .status(ReservationOutboxStatus.INIT)
                .message("{}")
                .createdAt(LocalDateTime.now())
                .build();
        outbox = outboxRepository.saveAndFlush(outbox);


        try {
            ReservationCommandMessage cmd = new ReservationCommandMessage(
                    outbox.getId(),
                    user.getId(),
                    concertDate.getId(),
                    seat.getId()
            );
            String json = objectMapper.writeValueAsString(cmd);
            outbox.setMessage(json);
            outboxRepository.save(outbox);
        } catch (Exception e) {
            throw new IllegalStateException("Outbox message 직렬화 실패", e);
        }

        return new ReservationResponse(
                outbox.getId(),
                reservationRequest.seatId(),
                "HOLDING_REQUESTED"
        );
    }
}


