package kr.hhplus.be.server.reservation.domain;
import jakarta.persistence.*;
import java.time.LocalDateTime;
import lombok.*;

@Entity
@Table(name = "outbox")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ReservationOutbox {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long reservationId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 30)
    private ReservationOutboxStatus status;


    @Lob
    @Column(nullable = false, columnDefinition = "longtext")
    private String message;

    @Column(nullable = false)
    private LocalDateTime createdAt;

    @Version
    private Long version;
}
