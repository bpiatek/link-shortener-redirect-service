package pl.bpiatek.linkshortenerredirectservice.link;

import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
interface WithRedis {

    @Container
    @ServiceConnection
    GenericContainer<?> redis = new GenericContainer<>(
            DockerImageName.parse("redis:8.2.1-alpine")
    ).withExposedPorts(6379);
}
