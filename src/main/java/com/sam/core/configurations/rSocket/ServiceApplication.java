package com.sam.core.configurations.rSocket;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.rsocket.RSocketSecurity;
import org.springframework.security.core.userdetails.MapReactiveUserDetailsService;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.messaging.handler.invocation.reactive.AuthenticationPrincipalArgumentResolver;
import org.springframework.security.rsocket.core.PayloadSocketAcceptorInterceptor;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Stream;


@Configuration
class SecurityConfiguration {

    @Bean
    PayloadSocketAcceptorInterceptor interceptor(RSocketSecurity security) {
        return security
                .simpleAuthentication(Customizer.withDefaults())
                .authorizePayload(ap -> ap.anyExchange().authenticated())
                .build();
    }

    @Bean
    MapReactiveUserDetailsService authentication() {
        return new MapReactiveUserDetailsService(
                User.withDefaultPasswordEncoder().username("jlong").password("pw").roles("USER").build());
    }

    @Bean
    RSocketMessageHandler messageHandler(RSocketStrategies strategies) {
        var rmh = new RSocketMessageHandler();
        rmh.getArgumentResolverConfigurer()
                .addCustomResolver(new AuthenticationPrincipalArgumentResolver());
        rmh.setRSocketStrategies(strategies);
        return rmh;
    }
}

// DTO
@Data
@AllArgsConstructor
@NoArgsConstructor
class GreetingResponse {
    private String message;
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class GreetingRequest {
    private String name;
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class BigRequest {
    private UUID id;
    private List<UUID> requests;
}

@Controller
@Log4j2
class GreetingController {

    private static BlockingQueue<BigRequest> queue = new LinkedBlockingDeque<>();

    @MessageMapping("channel")
    Flux<BigRequest> channel(RSocketRequester clientRSocketConnection, Flux<BigRequest> settings) {


        return Flux.create(
                (FluxSink<BigRequest> sink) -> {
                    settings.doFirst(()->{
                        pong(clientRSocketConnection);
                    })
                            .doOnNext(
                                    i -> {
                                        sink.next(i);
                                    })
                            .subscribe();
                });
    }

    private void pong(
            RSocketRequester clientRSocketConnection) {

        log.info("entra en el ping");
        Flux<String> pongSignal =
                Flux.fromStream(Stream.generate(() -> "ping")).delayElements(Duration.ofMillis(1000));

        var clientHealth =
                clientRSocketConnection
                        .route("amAlive")
                        //.data(pongSignal)
                        .retrieveFlux(String.class)
                        .doOnNext(chs -> log.info(chs)).subscribe();



    }


    @MessageMapping("startAmAlive")
    public Flux<String> startAmAlive(Flux<String> ping) {
        log.info("alive entering: ");
        Flux<String> pingSignal =
                Flux.fromStream(Stream.generate(() -> "pong")).delayElements(Duration.ofMillis(1500));

        return pingSignal;
    }
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class ClientHealthState {
    private boolean healthy;
}
