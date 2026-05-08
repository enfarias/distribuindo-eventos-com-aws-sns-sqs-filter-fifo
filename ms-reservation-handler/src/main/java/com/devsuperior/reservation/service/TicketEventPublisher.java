package com.devsuperior.reservation.service;

import com.devsuperior.reservation.event.TicketReservedEvent;
import io.awspring.cloud.sns.core.SnsHeaders;
import io.awspring.cloud.sns.core.SnsTemplate;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Wrapper fino sobre SnsTemplate para isolar a dependencia AWS do servico
 * de dominio. Serializa o evento como JSON via Jackson (necessario para
 * RawMessageDelivery=true permitir desserializacao direta nos consumers e
 * para o payload-based filter do SNS conseguir avaliar o body como JSON).
 * Propaga o X-Correlation-ID como header. No SNS FIFO, passa
 * MessageGroupId=showId e MessageDeduplicationId=reservationId via os
 * headers SnsHeaders.MESSAGE_GROUP_ID_HEADER e
 * SnsHeaders.MESSAGE_DEDUPLICATION_ID_HEADER.
 */
@Service
public class TicketEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(TicketEventPublisher.class);

    private static final String SOURCE = "ms-reservation-handler";

    private final SnsTemplate snsTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.topic.ticket-events}")
    private String ticketEventsTopic;

    public TicketEventPublisher(SnsTemplate snsTemplate, ObjectMapper objectMapper) {
        this.snsTemplate = snsTemplate;
        this.objectMapper = objectMapper;
    }

    public void publish(TicketReservedEvent event, String correlationId) {
        log.info("Publicando TicketReservedEvent reservationId={} showId={} tier={} no topico {}",
                event.reservationId(), event.showId(), event.ticketTier(), ticketEventsTopic);
        String payload;
        try {
            payload = objectMapper.writeValueAsString(event);
        } catch (JacksonException e) {
            throw new IllegalStateException(
                    "Falha ao serializar TicketReservedEvent reservationId=" + event.reservationId(), e);
        }
        Map<String, Object> headers = new HashMap<>();
        headers.put("X-Correlation-ID", correlationId != null ? correlationId : "NA");
        headers.put("X-Source", SOURCE);
        headers.put("contentType", "application/json");
        headers.put(SnsHeaders.MESSAGE_GROUP_ID_HEADER, event.showId());
        headers.put(SnsHeaders.MESSAGE_DEDUPLICATION_ID_HEADER, event.reservationId());
        snsTemplate.send(ticketEventsTopic,
                MessageBuilder.withPayload(payload).copyHeaders(headers).build());
        log.info("TicketReservedEvent reservationId={} publicado com sucesso", event.reservationId());
    }
}
