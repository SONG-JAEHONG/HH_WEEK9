package kr.hhplus.be.server.reservation.infra.persistence;

import java.util.List;
import kr.hhplus.be.server.reservation.domain.ReservationOutbox;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
public interface ReservationOutboxJpaRepository extends JpaRepository<ReservationOutbox, Long> {

    @Query(
            value = """
            SELECT * FROM outbox
             WHERE status = 'INIT'
             ORDER BY id
             LIMIT :limit
             FOR UPDATE SKIP LOCKED
        """,
            nativeQuery = true
    )
    List<ReservationOutbox> pickInitForPublish(@Param("limit") int limit);

}
