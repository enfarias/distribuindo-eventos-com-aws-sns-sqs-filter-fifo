package com.devsuperior.ingestor.config;

import io.awspring.cloud.sqs.operations.SqsTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

/**
 * Sobrescreve o {@link SqsTemplate} default apenas para desligar o envio do header
 * {@code JavaType} nas Message Attributes. Esse header carrega o FQCN do payload
 * (ex.: {@code com.devsuperior.ingestor.dto.ReservationRequestedEvent}) e polui a inspecao da
 * fila, alem de acoplar emissor e consumidor pelo nome da classe.
 */
@Configuration
public class SqsConfig {

    @Bean
    public SqsTemplate sqsTemplate(SqsAsyncClient sqsAsyncClient) {
        return SqsTemplate.builder()
                .sqsAsyncClient(sqsAsyncClient)
                .configureDefaultConverter(c -> c.doNotSendPayloadTypeHeader())
                .build();
    }
}
