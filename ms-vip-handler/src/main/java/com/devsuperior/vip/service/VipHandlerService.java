package com.devsuperior.vip.service;

import com.devsuperior.vip.event.TicketReservedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class VipHandlerService {

    private static final Logger log = LoggerFactory.getLogger(VipHandlerService.class);

    public void handlePremium(TicketReservedEvent event) {
        log.info("Credencial VIP emitida para reserva {} (show {}, tier {})",
                event.reservationId(), event.showId(), event.ticketTier());
        log.info("Concierge agendado para reserva {}, contato {}",
                event.reservationId(), event.buyerEmail());
    }
}
