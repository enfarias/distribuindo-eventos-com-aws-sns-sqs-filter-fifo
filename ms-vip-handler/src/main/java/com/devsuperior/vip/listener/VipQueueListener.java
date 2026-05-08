package com.devsuperior.vip.listener;

import com.devsuperior.vip.event.TicketReservedEvent;
import com.devsuperior.vip.service.VipHandlerService;
import io.awspring.cloud.sqs.annotation.SqsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class VipQueueListener {

    private static final Logger log = LoggerFactory.getLogger(VipQueueListener.class);

    private final VipHandlerService vipService;

    public VipQueueListener(VipHandlerService vipService) {
        this.vipService = vipService;
    }

    @SqsListener("${app.queue.vip}")
    public void onPremiumTicketReserved(
            TicketReservedEvent event,
            @Header(name = "X-Correlation-ID", required = false) String correlationId,
            @Header(name = "X-Source", required = false) String source) {
        try {
            MDC.put("correlationId", correlationId != null ? correlationId : "NA");
            MDC.put("source", source != null ? source : "NA");
            log.info("Reserva premium recebida da fila vip: reservationId={} tier={}",
                    event.reservationId(), event.ticketTier());
            vipService.handlePremium(event);
        } finally {
            MDC.clear();
        }
    }
}
