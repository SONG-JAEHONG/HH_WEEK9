package kr.hhplus.be.server.reservation.port.out;


import kr.hhplus.be.server.reservation.domain.ReservationOutbox;

import java.util.List;
import java.util.Optional;

public interface ReservationOutboxRepository {

    ReservationOutbox save(ReservationOutbox entity);
    ReservationOutbox saveAndFlush(ReservationOutbox entity);
    Optional<ReservationOutbox> findById(Long id);
    List<ReservationOutbox> pickInitForPublish(int limit);
}
