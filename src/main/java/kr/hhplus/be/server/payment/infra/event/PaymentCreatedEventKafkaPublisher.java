package kr.hhplus.be.server.payment.infra.event;


import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;
import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentCreatedEventKafkaPublisher {

    private static final String TOPIC = "payments.external.request";
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handle(PaymentCreatedEvent e) {
        PaymentExternalRequest payload = new PaymentExternalRequest(
                e.reservationId(),
                e.userId(),
                e.amount(),
                "SUCCESS",
                Instant.now()
        );


        String key = String.valueOf(e.reservationId());

        kafkaTemplate.send(TOPIC, key, payload)
                .whenComplete((result, ex) -> {
                    if (ex == null) {

                        log.info("Kafka publish OK topic={}, key={}, payload={}", TOPIC, key, payload);
                    } else {
                        log.error("Kafka publish FAIL topic={}, key={}, payload={}, reason={}",
                                TOPIC, key, payload, ex.toString(), ex);
                    }
                });
    }
}
