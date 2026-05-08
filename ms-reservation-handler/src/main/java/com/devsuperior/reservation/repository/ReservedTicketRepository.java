package com.devsuperior.reservation.repository;

import com.devsuperior.reservation.entity.ReservedTicket;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ReservedTicketRepository extends JpaRepository<ReservedTicket, Long> {

    boolean existsByReservationId(String reservationId);
}
