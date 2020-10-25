package com.sam.core.configurations.security;

import org.springframework.context.annotation.Bean;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.web.server.SecurityWebFilterChain;

@EnableWebFluxSecurity
public class ApplicationSecurityConfig {

  @Bean
  public SecurityWebFilterChain springSecurityFilterChain(ServerHttpSecurity http)  {
    // Disable login form
    http
            .httpBasic().disable()
            .formLogin().disable()
            .csrf().disable()
            .logout().disable();

    // Authorize everyone

    http
            .authorizeExchange()
            .anyExchange().permitAll();
    return http.build();
  }
}
