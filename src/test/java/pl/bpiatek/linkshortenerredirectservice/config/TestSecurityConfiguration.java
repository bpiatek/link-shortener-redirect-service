package pl.bpiatek.linkshortenerredirectservice.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({
        SecurityConfig.class,
        ActuatorSecurityFilterChain.class,
        PrometheusSecurityFilterChain.class
})
public class TestSecurityConfiguration {
}
