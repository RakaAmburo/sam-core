package com.sam.core.configurations.rSocket;

import com.sam.core.entities.Container;
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
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.UnicastProcessor;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
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

    private LinkedList<Container> queue = new LinkedList<>();
    private UnicastProcessor<BigRequest> requestStream;
    private UnicastProcessor<BigRequest> responseStream;
    private FluxSink<BigRequest> requestSink;
    private FluxSink<BigRequest> responseSink;

    private Disposable ping;
    private RSocketRequester client;

    @MessageMapping("startPing")
    Flux<String> startPing() {


        Flux<String> pingSignal =
                Flux.fromStream(Stream.generate(() -> "ping")).delayElements(Duration.ofMillis(1000));


        return pingSignal;
    }

    @MessageMapping("channel")
    Flux<BigRequest> channel(RSocketRequester clientRSocketConnection, Flux<BigRequest> bigRequestFlux) {

        System.out.println("instanciamos");

        /*bigRequestFlux.doOnNext(bigRequest -> {
            synchronized (this) {
                this.queue.add(new Container());
                this.requestSink.next(bigRequest);
            }
        });

        return responseStream;*/


        return Flux.create(
                (FluxSink<BigRequest> sink) -> {
                    bigRequestFlux
                            .doOnNext(
                                    i -> {
                                        sink.next(i);
                                    })
                            .subscribe();
                });
    }
}
