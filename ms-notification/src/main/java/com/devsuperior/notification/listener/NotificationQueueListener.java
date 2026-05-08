package com.devsuperior.notification.listener;

import com.devsuperior.notification.event.TicketReservedEvent;
import com.devsuperior.notification.service.NotificationBroadcaster;
import io.awspring.cloud.sqs.annotation.SqsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class NotificationQueueListener {

    private static final Logger log = LoggerFactory.getLogger(NotificationQueueListener.class);

    private final NotificationBroadcaster broadcaster;

    public NotificationQueueListener(NotificationBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    @SqsListener("${app.queue.notification}")
    public void onTicketReserved(
            TicketReservedEvent event,
            @Header(name = "X-Correlation-ID", required = false) String correlationId,
            @Header(name = "X-Source", required = false) String source) {
        try {
            MDC.put("correlationId", correlationId != null ? correlationId : "NA");
            MDC.put("source", source != null ? source : "NA");
            log.info("Notificacao recebida da fila para reserva {} (show {}, tier {})",
                    event.reservationId(), event.showId(), event.ticketTier());
            broadcaster.broadcast(event);
        } finally {
            MDC.clear();
        }
    }
}
