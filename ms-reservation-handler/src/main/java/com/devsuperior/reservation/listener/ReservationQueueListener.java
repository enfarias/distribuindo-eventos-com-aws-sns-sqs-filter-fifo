package com.devsuperior.reservation.listener;

import com.devsuperior.reservation.dto.ReservationRequestedEvent;
import com.devsuperior.reservation.service.ReservationProcessorService;
import io.awspring.cloud.sqs.annotation.SqsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class ReservationQueueListener {

    private static final Logger log = LoggerFactory.getLogger(ReservationQueueListener.class);

    private final ReservationProcessorService processorService;

    public ReservationQueueListener(ReservationProcessorService processorService) {
        this.processorService = processorService;
    }

    @SqsListener("${app.queue.reservation}")
    public void onReservationRequested(
            ReservationRequestedEvent event,
            @Header(name = "X-Correlation-ID", required = false) String correlationId,
            @Header(name = "X-Source", required = false) String source) {
        try {
            MDC.put("correlationId", correlationId != null ? correlationId : "NA");
            MDC.put("source", source != null ? source : "NA");
            log.info("Reserva recebida da fila: reservationId={} showId={}",
                    event.reservationId(), event.showId());
            processorService.process(event, correlationId);
            log.info("Reserva {} processada com sucesso", event.reservationId());
        } finally {
            MDC.clear();
        }
    }
}
