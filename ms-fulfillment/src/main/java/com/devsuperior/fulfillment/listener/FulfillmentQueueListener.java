package com.devsuperior.fulfillment.listener;

import com.devsuperior.fulfillment.event.TicketReservedEvent;
import com.devsuperior.fulfillment.service.FulfillmentService;
import io.awspring.cloud.sqs.annotation.SqsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class FulfillmentQueueListener {

    private static final Logger log = LoggerFactory.getLogger(FulfillmentQueueListener.class);

    private final FulfillmentService fulfillmentService;

    public FulfillmentQueueListener(FulfillmentService fulfillmentService) {
        this.fulfillmentService = fulfillmentService;
    }

    @SqsListener("${app.queue.fulfillment}")
    public void onTicketReserved(
            TicketReservedEvent event,
            @Header(name = "X-Correlation-ID", required = false) String correlationId,
            @Header(name = "X-Source", required = false) String source) {
        try {
            MDC.put("correlationId", correlationId != null ? correlationId : "NA");
            MDC.put("source", source != null ? source : "NA");
            log.info("Fulfillment recebido para reserva {}", event.reservationId());
            fulfillmentService.releaseTickets(event);
        } finally {
            MDC.clear();
        }
    }
}
