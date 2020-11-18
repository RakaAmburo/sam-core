package com.sam.core.controllers;

import com.sam.commons.entities.BigRequest;
import com.sam.core.entities.Container;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.metadata.WellKnownMimeType;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.security.rsocket.metadata.UsernamePasswordMetadata;
import org.springframework.stereotype.Controller;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Controller
@Log4j2
class CoreController {

    private final UsernamePasswordMetadata credentials = new UsernamePasswordMetadata("jlong", "pw");
    private final MimeType mimeType =
            MimeTypeUtils.parseMimeType(WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.getString());
    private LinkedList<Container> queue = new LinkedList<>();
    private UnicastProcessor<BigRequest> requestStream;
    private FluxSink<BigRequest> requestSink;
    private Disposable ping;
    private RSocketRequester client;
    private ScheduledExecutorService shutDown = Executors.newSingleThreadScheduledExecutor();
    private Disposable connection;
    private RSocketRequester.Builder rSocketBuilder;
    private Disposable pingSubscription;
    private boolean connected = false;
    private boolean connecting = false;
    private Long pingTime = 0L;
    @Value("${core.RSocket.host:localhost}")
    private String coreRSocketHost;
    @Value("${core.RSocket.port:8888}")
    private Integer coreRSocketPort;
    public CoreController(RSocketRequester.Builder rSocketBuilder) {
        this.rSocketBuilder = rSocketBuilder;
        this.shutDown.scheduleAtFixedRate(this.checkServerPing(), 1000, 1000, TimeUnit.MILLISECONDS);

    }

    @MessageMapping("startPing")
    Flux<String> startPing() {


        Flux<String> pingSignal =
                Flux.fromStream(Stream.generate(() -> "ping")).delayElements(Duration.ofMillis(1000));


        return pingSignal;
    }

    @MessageMapping("channel")
    Flux<BigRequest> channel(RSocketRequester clientRSocketConnection, Flux<BigRequest> bigRequestFlux) {

        System.out.println("channel connect to mongo");
        UnicastProcessor<BigRequest> responseStream = UnicastProcessor.create();
        FluxSink<BigRequest> responseSink = responseStream.sink();

        bigRequestFlux.doOnNext(bigRequest -> {

            Container container = new Container(responseSink);
            synchronized (this) {
                //System.out.println("enviamos al mongo");
                this.queue.add(container);
                this.requestSink.next(bigRequest);
            }
        }).subscribe();

        return responseStream;


        /*return Flux.create(
                (FluxSink<BigRequest> sink) -> {
                    bigRequestFlux
                            .doOnNext(
                                    i -> {
                                        System.out.println("enviamos al mongo (supeustamente)");
                                        sink.next(i);
                                    })
                            .subscribe();
                });*/
    }

    private void startPingOut() {
        pingSubscription =
                client
                        .route("startPing")
                        .metadata(this.credentials, this.mimeType)
                        .data(Mono.empty())
                        .retrieveFlux(String.class)
                        .doOnNext(
                                ping -> {
                                    if (!connected) {
                                        System.out.println("pinging now connecting");
                                        connect();
                                        connected = true;
                                        connecting = false;
                                    }
                                    pingTime = System.currentTimeMillis();
                                })
                        .subscribe();
    }

    private void getRSocketRequester() {
        this.client =
                this.rSocketBuilder
                        .setupMetadata(this.credentials, this.mimeType)
                        // .rsocketConnector(connector -> connector.acceptor(acceptor))
                        .rsocketConnector(
                                connector -> {
                                    //connector.acceptor(acceptor);
                                    connector.payloadDecoder(PayloadDecoder.ZERO_COPY);
                                    //connector.reconnect(Retry.fixedDelay(Integer.MAX_VALUE, Duration.ofSeconds(1)));
                                })
                        // .reconnect(Retry.fixedDelay(Integer.MAX_VALUE, Duration.ofSeconds(5)))
                        .connectTcp(coreRSocketHost, coreRSocketPort)
                        .doOnSuccess(
                                success -> {
                                    System.out.println("Socket Connected!");
                                })
                        .doOnError(
                                error -> {
                                    System.out.println(error);
                                })
                        .retryWhen(
                                Retry.fixedDelay(Integer.MAX_VALUE, Duration.ofSeconds(1))
                                        .doAfterRetry(
                                                signal -> {
                                                    // log.info("Retrying times:  " + signal.totalRetriesInARow());
                                                }))
                        .block();
    }

    private void connect() {
        if (this.requestStream != null) {
            requestStream.sink().complete();
            requestStream = null;
        }
        requestStream = UnicastProcessor.create();
        this.requestSink = requestStream.sink();

        connection =
                this.client
                        .route("mongoChannel")
                        .metadata(this.credentials, this.mimeType)
                        // .data(Mono.empty())
                        .data(requestStream)
                        .retrieveFlux(BigRequest.class)
                        .retryWhen(Retry.fixedDelay(Integer.MAX_VALUE, Duration.ofSeconds(1)))
                        .doOnError(
                                error -> {
                                    System.out.println("Error sending data: " + error);
                                })
                        .doOnNext(
                                bigRequest -> {
                                    queue.pop().getSink().next(bigRequest);
                                    // System.out.println("ID: " + bigRequest.getId());
                                })
                        .subscribe();
    }

    public Runnable checkServerPing() {
        return () -> {
            // System.out.println("QUEUE SIZE = " + this.queue.size());
            if (connected) {
                Long now = System.currentTimeMillis();
                Long diff = now - pingTime;

                if (diff > 1200) {
                    System.out.println(diff + " too long diff, reconnecting!");
                    connected = false;
                }
            }

            if (!connected && !connecting) {
                connecting = true;
                System.out.println("connecting process");
                if (this.client != null) {
                    this.client.rsocket().dispose();
                    this.client = null;
                }
                if (connection != null) {
                    connection.dispose();
                    connection = null;
                }

                if (pingSubscription != null) {
                    pingSubscription.dispose();
                    pingSubscription = null;
                }

                getRSocketRequester();
                startPingOut();
            }


        };
    }
}
