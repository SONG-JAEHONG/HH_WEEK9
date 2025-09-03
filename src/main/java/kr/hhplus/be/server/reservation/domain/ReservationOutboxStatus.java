package kr.hhplus.be.server.reservation.domain;

public enum ReservationOutboxStatus {

    INIT,
    RECEIPT,
    SUCCESS,
    ROLLBACK_RECEIPT,
    ROLLBACK_DONE
}
