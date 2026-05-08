package com.devsuperior.reservation.event;

import java.math.BigDecimal;
import java.time.Instant;

public record TicketReservedEvent(
        String reservationId,
        String showId,
        String ticketTier,
        Integer quantity,
        BigDecimal totalAmountUsd,
        String buyerEmail,
        Instant reservedAt
) {
}
