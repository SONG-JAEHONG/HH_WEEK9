package kr.hhplus.be.server.reservation.infra.persistence;

import java.util.List;
import java.util.Optional;

import kr.hhplus.be.server.reservation.domain.ReservationOutbox;
import kr.hhplus.be.server.reservation.port.out.ReservationOutboxRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class ReservationOutboxRepositoryAdapter implements ReservationOutboxRepository {

    private final ReservationOutboxJpaRepository jpaRepository;

    @Override
    @Transactional
    public ReservationOutbox save(ReservationOutbox entity) {
        return jpaRepository.save(entity);
    }

    @Override
    @Transactional
    public ReservationOutbox saveAndFlush(ReservationOutbox entity) {
        return jpaRepository.saveAndFlush(entity);
    }

    @Override
    public Optional<ReservationOutbox> findById(Long id) {
        return jpaRepository.findById(id);
    }

    @Override
    @Transactional
    public List<ReservationOutbox> pickInitForPublish(int limit) {
        return jpaRepository.pickInitForPublish(limit);
    }
}