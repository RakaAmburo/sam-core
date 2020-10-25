package com.sam.core.controllers;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/test")
public class Test {

    @GetMapping()
    Flux<String> getAll() {
        System.out.println("test");
        return Flux.just("Its working!");
    }
}
