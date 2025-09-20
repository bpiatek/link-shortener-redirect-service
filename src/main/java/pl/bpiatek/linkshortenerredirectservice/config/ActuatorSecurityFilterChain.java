package pl.bpiatek.linkshortenerredirectservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.web.SecurityFilterChain;

import static org.springframework.security.config.Customizer.withDefaults;
import static org.springframework.security.config.http.SessionCreationPolicy.STATELESS;

@Configuration
class ActuatorSecurityFilterChain {

    @Bean
    @Order(2)
    SecurityFilterChain actuatorFilterChain(HttpSecurity http) throws Exception {
        http
                .securityMatcher("/actuator/**")
                .authorizeHttpRequests(auth -> auth
                        .anyRequest().hasRole("MONITORING")
                )
                .httpBasic(withDefaults())
                .sessionManagement(session -> session.sessionCreationPolicy(STATELESS))
                .csrf(AbstractHttpConfigurer::disable);

        return http.build();
    }
}
