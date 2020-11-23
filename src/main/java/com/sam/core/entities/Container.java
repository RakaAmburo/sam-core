package com.sam.core.entities;

import com.sam.commons.entities.BigRequest;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import reactor.core.publisher.FluxSink;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Container<T> {
    private FluxSink<T> sink;
}
