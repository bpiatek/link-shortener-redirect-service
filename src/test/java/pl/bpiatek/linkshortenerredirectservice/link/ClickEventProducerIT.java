package pl.bpiatek.linkshortenerredirectservice.link;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.*;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

@SpringBootTest
@ActiveProfiles("test")
class ClickEventProducerIT implements WithFullInfrastructure{

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", redpanda::getBootstrapServers);
        registry.add("spring.kafka.consumer.properties.specific.protobuf.value.type",
                () -> "pl.bpiatek.contracts.link.LinkClickEventProto$LinkClickEvent");
    }

    @Autowired
    private ClickEventProducer clickEventProducer;

    @Autowired
    private TestClickEventConsumer testConsumer;

    @AfterEach
    void cleanup() {
        testConsumer.reset();
    }

    @Test
    void shouldProduceAndConsumeClickEvent() throws InterruptedException {
        // given
        var shortCode = "click123";
        var userAgent = "Mozilla/5.0 (Test Integration)";
        var ipAddress = "192.168.1.100";

        var request = new MockHttpServletRequest();
        request.addHeader("User-Agent", userAgent);
        request.setRemoteAddr(ipAddress);

        // when
        clickEventProducer.sendClickEvent(shortCode, request);

        // then
        var record = testConsumer.awaitRecord(10, TimeUnit.SECONDS);
        assertSoftly(softly -> {
            softly.assertThat(record).as("Consumed Kafka record").isNotNull();
            softly.assertThat(record.key()).as("Kafka message key").isEqualTo(shortCode);

            var message = record.value();
            softly.assertThat(message.getShortUrl()).isEqualTo(shortCode);
            softly.assertThat(message.getIpAddress()).isEqualTo(ipAddress);
            softly.assertThat(message.getUserAgent()).isEqualTo(userAgent);

            var headers = record.headers();
            softly.assertThat(new String(headers.lastHeader("source").value(), UTF_8))
                    .as("Kafka 'source' header").isEqualTo("redirect-service");
            softly.assertThat(headers.lastHeader("trace-id").value()).as("Kafka 'trace-id' header").isNotNull();
        });
    }
}