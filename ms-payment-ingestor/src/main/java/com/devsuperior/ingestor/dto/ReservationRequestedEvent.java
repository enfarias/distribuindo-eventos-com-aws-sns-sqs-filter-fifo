package com.devsuperior.ingestor.dto;

import java.math.BigDecimal;
import java.time.Instant;

public record ReservationRequestedEvent(
        String reservationId,
        String showId,
        String ticketTier,
        Integer quantity,
        BigDecimal unitPriceUsd,
        String buyerEmail,
        String buyerName,
        Instant requestedAt
) {
}
