package pl.bpiatek.linkshortenerredirectservice.config;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component("vault")
class VaultHealthIndicator implements HealthIndicator {
    private final RestClient vaultRestClient;

    VaultHealthIndicator(RestClient vaultRestClient) {
        this.vaultRestClient = vaultRestClient;
    }

    @Override
    public Health health() {
        try {
            var response = vaultRestClient.get()
                    .uri("/v1/sys/health")
                    .retrieve()
                    .toBodilessEntity();

            return switch (response.getStatusCode().value()) {
                case 200 -> Health.up().withDetail("state", "ACTIVE").build();
                case 503 -> Health.down().withDetail("state", "SEALED").build();
                default -> Health.down().withDetail("http_status", response.getStatusCode()).build();
            };
        } catch (Exception e) {
            return Health.down(e).withDetail("error", "Vault unreachable").build();
        }
    }
}