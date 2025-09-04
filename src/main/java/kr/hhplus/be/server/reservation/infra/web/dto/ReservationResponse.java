package kr.hhplus.be.server.reservation.infra.web.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ReservationResponse {
    private Long trackingId;
    private Long seatId;
    private String status;
}
