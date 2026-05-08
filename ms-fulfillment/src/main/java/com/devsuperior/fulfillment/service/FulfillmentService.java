package com.devsuperior.fulfillment.service;

import com.devsuperior.fulfillment.event.TicketReservedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class FulfillmentService {

    private static final Logger log = LoggerFactory.getLogger(FulfillmentService.class);

    public void releaseTickets(TicketReservedEvent event) {
        log.info("Liberando ingressos da reserva {} (show {}, tier {}, {} ingressos, total {} USD)",
                event.reservationId(),
                event.showId(),
                event.ticketTier(),
                event.quantity(),
                event.totalAmountUsd());
    }
}
