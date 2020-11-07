package com.sam.core.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import reactor.core.publisher.FluxSink;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Container {
    private FluxSink<BigRequest> sink;
}
