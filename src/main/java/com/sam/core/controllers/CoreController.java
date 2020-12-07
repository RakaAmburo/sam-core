package com.sam.core.controllers;

import com.sam.commons.entities.BigRequest;
import com.sam.commons.entities.MenuItemReq;
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
  private LinkedList<Container<BigRequest>> queue = new LinkedList<>();
  private LinkedList<Container<MenuItemReq>> menuItemQueue = new LinkedList<>();
  private LinkedList<Container<MenuItemReq>> deleteMenuItemQueue = new LinkedList<>();
  private UnicastProcessor<BigRequest> requestStream;
  private UnicastProcessor<MenuItemReq> menuItemReqStr;
  private UnicastProcessor<MenuItemReq> deleteMenuItemReqStr;
  private FluxSink<BigRequest> requestSink;
  private FluxSink<MenuItemReq> menuItemReqStrSink;
  private FluxSink<MenuItemReq> deleteMenuItemReqStrSink;
  private Disposable ping;
  private RSocketRequester client;
  private ScheduledExecutorService shutDown = Executors.newSingleThreadScheduledExecutor();
  private Disposable connection;
  private Disposable menuItemReqConnection;
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
  public Flux<BigRequest> channel(
      RSocketRequester clientRSocketConnection, Flux<BigRequest> bigRequestFlux) {

    System.out.println("channel connect to mongo");
    UnicastProcessor<BigRequest> responseStream = UnicastProcessor.create();
    FluxSink<BigRequest> responseSink = responseStream.sink();

    bigRequestFlux
        .doOnNext(
            bigRequest -> {
              Container<BigRequest> container = new Container(responseSink);
              synchronized (this) {
                // System.out.println("enviamos al mongo");
                this.queue.add(container);
                this.requestSink.next(bigRequest);
              }
            })
        .subscribe();

    return responseStream;
  }

  @MessageMapping("menuItemReqChannel")
  public Flux<MenuItemReq> menuItemReqChannel(Flux<MenuItemReq> menuItemFlux) {

    return genericChannel(menuItemFlux, this.menuItemQueue, this.menuItemReqStrSink);
  }

  @MessageMapping("deleteMenuItemReqChannel")
  public Flux<MenuItemReq> deleteMenuItemReqChannel(Flux<MenuItemReq> menuItemFlux) {
    return genericChannel(menuItemFlux, this.deleteMenuItemQueue, this.deleteMenuItemReqStrSink);
  }

  private Flux<MenuItemReq> genericChannel(
      Flux<MenuItemReq> menuItemFlux,
      LinkedList<Container<MenuItemReq>> queueAux,
      FluxSink<MenuItemReq> sinkAux) {
    System.out.println("channel connect to menuitem mongo");
    UnicastProcessor<MenuItemReq> responseStream = UnicastProcessor.create();
    FluxSink<MenuItemReq> responseSinkAux = responseStream.sink();

    menuItemFlux
        .doOnNext(
            bigRequest -> {
              Container<MenuItemReq> container = new Container(responseSinkAux);
              synchronized (this) {
                System.out.println("enviamos al mongo");
                queueAux.add(container);
                sinkAux.next(bigRequest);
              }
            })
        .subscribe();

    return responseStream;
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
                    connectMenuitem();
                    deleteMenuItemConnect();
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
                  // connector.acceptor(acceptor);
                  connector.payloadDecoder(PayloadDecoder.ZERO_COPY);
                  // connector.reconnect(Retry.fixedDelay(Integer.MAX_VALUE,
                  // Duration.ofSeconds(1)));
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

  private void connectMenuitem() {
    if (this.menuItemReqStr != null) {
      this.menuItemReqStr.sink().complete();
      this.menuItemReqStr = null;
    }
    this.menuItemReqStr = UnicastProcessor.create();
    this.menuItemReqStrSink = this.menuItemReqStr.sink();

    menuItemReqConnection =
        this.client
            .route("menuItemChannel")
            .metadata(this.credentials, this.mimeType)
            .data(menuItemReqStr)
            .retrieveFlux(MenuItemReq.class)
            .retryWhen(Retry.fixedDelay(Integer.MAX_VALUE, Duration.ofSeconds(1)))
            .doOnNext(
                request -> {
                  menuItemQueue.pop().getSink().next(request);
                  // System.out.println("ID: " + bigRequest.getId());
                })
            .subscribe();
  }

  private void deleteMenuItemConnect() {
    if (this.deleteMenuItemReqStr != null) {
      this.deleteMenuItemReqStr.sink().complete();
      this.deleteMenuItemReqStr = null;
    }
    this.deleteMenuItemReqStr = UnicastProcessor.create();
    this.deleteMenuItemReqStrSink = this.deleteMenuItemReqStr.sink();

    menuItemReqConnection =
        this.client
            .route("deleteMenuItemChannel")
            .metadata(this.credentials, this.mimeType)
            .data(deleteMenuItemReqStr)
            .retrieveFlux(MenuItemReq.class)
            .retryWhen(Retry.fixedDelay(Integer.MAX_VALUE, Duration.ofSeconds(1)))
            .doOnNext(
                request -> {
                  deleteMenuItemQueue.pop().getSink().next(request);
                  // System.out.println("ID: " + bigRequest.getId());
                })
            .subscribe();
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
