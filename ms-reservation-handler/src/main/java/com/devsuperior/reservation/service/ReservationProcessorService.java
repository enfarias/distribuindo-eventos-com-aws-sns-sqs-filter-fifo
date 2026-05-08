package com.devsuperior.reservation.service;

import com.devsuperior.reservation.dto.ReservationRequestedEvent;
import com.devsuperior.reservation.entity.ReservedTicket;
import com.devsuperior.reservation.event.TicketReservedEvent;
import com.devsuperior.reservation.repository.ReservedTicketRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;

@Service
public class ReservationProcessorService {

    private static final Logger log = LoggerFactory.getLogger(ReservationProcessorService.class);

    private static final BigDecimal SERVICE_FEE_RATE = new BigDecimal("0.08");

    private final ReservedTicketRepository repository;
    private final ShowInventoryService inventoryService;
    private final TicketEventPublisher eventPublisher;

    public ReservationProcessorService(ReservedTicketRepository repository,
                                       ShowInventoryService inventoryService,
                                       TicketEventPublisher eventPublisher) {
        this.repository = repository;
        this.inventoryService = inventoryService;
        this.eventPublisher = eventPublisher;
    }

    public void process(ReservationRequestedEvent event, String correlationId) {
        if (repository.existsByReservationId(event.reservationId())) {
            log.warn("Reserva {} ja foi processada, ignorando duplicata", event.reservationId());
            return;
        }

        if (!inventoryService.hasAvailability(event.showId(), event.quantity())) {
            log.warn("Sem disponibilidade para showId={} quantity={}, reserva {} rejeitada",
                    event.showId(), event.quantity(), event.reservationId());
            return;
        }

        BigDecimal subtotal = event.unitPriceUsd().multiply(BigDecimal.valueOf(event.quantity()));
        BigDecimal serviceFee = subtotal.multiply(SERVICE_FEE_RATE).setScale(2, RoundingMode.HALF_UP);
        BigDecimal totalAmount = subtotal.add(serviceFee).setScale(2, RoundingMode.HALF_UP);

        log.info("Calculo de valor para reserva {}: subtotal={} taxa={} total={}",
                event.reservationId(), subtotal, serviceFee, totalAmount);

        ReservedTicket reserved = new ReservedTicket(
                event.reservationId(),
                event.showId(),
                event.ticketTier(),
                event.quantity(),
                event.unitPriceUsd(),
                totalAmount,
                event.buyerEmail(),
                "RESERVED",
                Instant.now()
        );
        repository.save(reserved);

        log.info("Reserva persistida: reservationId={} total={} USD",
                reserved.getReservationId(), totalAmount);

        TicketReservedEvent domainEvent = new TicketReservedEvent(
                reserved.getReservationId(),
                reserved.getShowId(),
                reserved.getTicketTier(),
                reserved.getQuantity(),
                reserved.getTotalAmountUsd(),
                reserved.getBuyerEmail(),
                reserved.getReservedAt()
        );
        eventPublisher.publish(domainEvent, correlationId);
    }
}
