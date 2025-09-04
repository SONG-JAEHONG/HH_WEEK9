package kr.hhplus.be.server.reservation.application;


import java.util.List;

import kr.hhplus.be.server.reservation.domain.ReservationOutbox;
import kr.hhplus.be.server.reservation.port.out.ReservationOutboxRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;


@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.kafka", name = "enabled", havingValue = "true")
public class OutboxPublisher {

    private final ReservationOutboxRepository outboxRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final int BATCH = 200;
    private static final String RESERVATION_TOPIC = "reservation";

    @Scheduled(fixedDelay = 500L)
    @Transactional
    public void publishInit() {
        List<ReservationOutbox> rows = outboxRepository.pickInitForPublish(BATCH);
        for (ReservationOutbox outboxEntity : rows) {
            kafkaTemplate.send(RESERVATION_TOPIC, outboxEntity.getMessage());

        }
    }
}
