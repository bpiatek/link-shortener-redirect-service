package pl.bpiatek.linkshortenerredirectservice.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component("kafka")
class KafkaHealthIndicator implements HealthIndicator {

    private static final Logger log = LoggerFactory.getLogger(KafkaHealthIndicator.class);
    private final AdminClient adminClient;

    KafkaHealthIndicator(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    @Override
    public Health health() {
        try {
            var options = new DescribeClusterOptions().timeoutMs(3000);
            var nodes = adminClient.describeCluster(options)
                    .nodes()
                    .get(3, TimeUnit.SECONDS);

            if (nodes.isEmpty()) {
                log.warn("Kafka health check failed: Cluster exists but has no nodes");
                return Health.down().withDetail("error", "No nodes found in cluster").build();
            }

            return Health.up().withDetail("nodeCount", nodes.size()).build();

        } catch (Exception e) {
            log.warn("Kafka health check failed: {}", e.getMessage());
            return Health.down(e).withDetail("error", "Cannot connect to Kafka broker").build();
        }
    }
}