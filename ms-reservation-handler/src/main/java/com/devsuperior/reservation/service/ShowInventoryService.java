package com.devsuperior.reservation.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ShowInventoryService {

    private static final Logger log = LoggerFactory.getLogger(ShowInventoryService.class);

    public boolean hasAvailability(String showId, Integer quantity) {
        log.debug("Validando disponibilidade para showId={} quantity={}", showId, quantity);
        return true;
    }
}
