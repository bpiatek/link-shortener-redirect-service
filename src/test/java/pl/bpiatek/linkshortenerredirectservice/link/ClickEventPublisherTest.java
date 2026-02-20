package pl.bpiatek.linkshortenerredirectservice.link;

import com.google.protobuf.Timestamp;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import pl.bpiatek.contracts.link.LinkClickEventProto.LinkClickEvent;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static java.nio.charset.StandardCharsets.*;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ClickEventPublisherTest {

    private static final String TEST_TOPIC = "test-link-clicks";
    private final Instant now =Instant.parse("2025-08-22T10:00:00Z");

    @Mock
    private KafkaTemplate<String, LinkClickEvent> kafkaTemplate;

    @Mock
    private Clock clock;

    @Mock
    private ClickEventPublisher clickEventPublisher;

    @Captor
    private ArgumentCaptor<ProducerRecord<String, LinkClickEvent>> producerRecordCaptor;


    @BeforeEach
    void setUp() {
        clickEventPublisher = new ClickEventPublisher(kafkaTemplate, TEST_TOPIC, clock);
        lenient().when(clock.instant()).thenReturn(now);
    }

    @Test
    void shouldBuildAndSendCorrectClickEvent() {
        // given
        var shortCode = "aB5xZ1";
        var userAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)";
        var ipAddress = "123.123.123.123";

        given(kafkaTemplate.send(any(ProducerRecord.class)))
                .willReturn(CompletableFuture.completedFuture(null));

        // when
        clickEventPublisher.publishSafe(shortCode, ipAddress, userAgent);

        // then
        verify(kafkaTemplate, timeout(1000)).send(producerRecordCaptor.capture());
        var capturedRecord = producerRecordCaptor.getValue();

        assertSoftly(s -> {
            s.assertThat(capturedRecord.topic()).isEqualTo(TEST_TOPIC);
            s.assertThat(capturedRecord.key()).isEqualTo(shortCode);

            var event = capturedRecord.value();
            s.assertThat(event.getShortUrl()).isEqualTo(shortCode);
            s.assertThat(event.getIpAddress()).isEqualTo(ipAddress);
            s.assertThat(event.getUserAgent()).isEqualTo(userAgent);
            s.assertThat(event.getClickedAt()).isEqualTo(
                    Timestamp.newBuilder()
                            .setSeconds(now.getEpochSecond())
                            .setNanos(now.getNano())
                            .build()
            );
            s.assertThat(new String(capturedRecord.headers().lastHeader("source").value(), UTF_8))
                    .isEqualTo("redirect-service");
        });
    }
}