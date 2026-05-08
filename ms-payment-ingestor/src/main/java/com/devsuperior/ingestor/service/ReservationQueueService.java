package com.devsuperior.ingestor.service;

import com.devsuperior.ingestor.dto.ReservationRequestedEvent;
import io.awspring.cloud.sqs.listener.SqsHeaders;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class ReservationQueueService {

    private static final Logger log = LoggerFactory.getLogger(ReservationQueueService.class);

    private static final String SOURCE = "ms-payment-ingestor";

    private final SqsTemplate sqsTemplate;

    @Value("${app.queue.reservation}")
    private String reservationQueue;

    public ReservationQueueService(SqsTemplate sqsTemplate) {
        this.sqsTemplate = sqsTemplate;
    }

    public void enqueue(ReservationRequestedEvent event, String correlationId) {
        log.info("Enfileirando reserva {} (show {}) na reservation-queue.fifo",
                event.reservationId(), event.showId());
        sqsTemplate.send(to -> to
                .queue(reservationQueue)
                .payload(event)
                .header("X-Correlation-ID", correlationId)
                .header("X-Source", SOURCE)
                .header(SqsHeaders.MessageSystemAttributes.SQS_MESSAGE_GROUP_ID_HEADER, event.showId())
                .header(SqsHeaders.MessageSystemAttributes.SQS_MESSAGE_DEDUPLICATION_ID_HEADER, event.reservationId()));
        log.info("Reserva {} enfileirada com MessageGroupId={} MessageDeduplicationId={}",
                event.reservationId(), event.showId(), event.reservationId());
    }
}
