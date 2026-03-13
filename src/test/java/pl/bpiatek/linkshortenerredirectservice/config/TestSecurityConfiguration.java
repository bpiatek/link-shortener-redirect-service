package pl.bpiatek.linkshortenerredirectservice.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({
        SecurityConfig.class,
        ActuatorSecurityFilterChain.class,
        PublicActuatorSecurityFilterChainConfig.class
})
public class TestSecurityConfiguration {
}
