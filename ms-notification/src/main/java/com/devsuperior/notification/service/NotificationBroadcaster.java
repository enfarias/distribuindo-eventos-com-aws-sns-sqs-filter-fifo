package com.devsuperior.notification.service;

import com.devsuperior.notification.event.TicketReservedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Mantem listas de SseEmitter por reservationId para entregar cada evento apenas
 * aos clientes que se conectaram ao stream daquela reserva. As entries do
 * map sao removidas quando a lista correspondente fica vazia, evitando leak
 * ao longo do dia.
 */
@Service
public class NotificationBroadcaster {

    private static final Logger log = LoggerFactory.getLogger(NotificationBroadcaster.class);

    private final Map<String, CopyOnWriteArrayList<SseEmitter>> emittersByReservationId = new ConcurrentHashMap<>();

    public void register(String reservationId, SseEmitter emitter) {
        emittersByReservationId
                .computeIfAbsent(reservationId, key -> new CopyOnWriteArrayList<>())
                .add(emitter);

        emitter.onCompletion(() -> removeEmitter(reservationId, emitter, "completion"));
        emitter.onTimeout(() -> removeEmitter(reservationId, emitter, "timeout"));
    }

    public void broadcast(TicketReservedEvent event) {
        List<SseEmitter> targets = emittersByReservationId.get(event.reservationId());
        if (targets == null || targets.isEmpty()) {
            log.info("Nenhum cliente conectado ao stream da reserva {}, evento descartado para SSE",
                    event.reservationId());
            return;
        }

        log.info("Broadcast da reserva {} para {} cliente(s) conectado(s)",
                event.reservationId(), targets.size());

        for (SseEmitter emitter : targets) {
            try {
                emitter.send(SseEmitter.event()
                        .name("ticket-reserved")
                        .data(event));
            } catch (IOException e) {
                log.warn("Falha ao enviar evento para emitter da reserva {}, removendo: {}",
                        event.reservationId(), e.getMessage());
                removeEmitter(event.reservationId(), emitter, "io-error");
            }
        }
    }

    private void removeEmitter(String reservationId, SseEmitter emitter, String reason) {
        CopyOnWriteArrayList<SseEmitter> list = emittersByReservationId.get(reservationId);
        if (list == null) {
            return;
        }
        list.remove(emitter);
        if (list.isEmpty()) {
            emittersByReservationId.remove(reservationId, list);
        }
        log.info("Emitter da reserva {} removido (motivo: {}), restam {}",
                reservationId, reason, list.size());
    }
}
