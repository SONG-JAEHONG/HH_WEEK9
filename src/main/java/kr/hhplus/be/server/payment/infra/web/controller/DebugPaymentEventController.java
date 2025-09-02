package kr.hhplus.be.server.payment.infra.web.controller;

import kr.hhplus.be.server.payment.infra.event.PaymentCreatedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/debug")
public class DebugPaymentEventController {

    private final ApplicationEventPublisher publisher;

    @PostMapping("/payment-event")
    @Transactional
    public String triggerPaymentEvent(@RequestParam Long reservationId,
                                      @RequestParam Long userId,
                                      @RequestParam Long amount) {
        publisher.publishEvent(new PaymentCreatedEvent(reservationId, userId, amount));
        return "published";
    }
}
