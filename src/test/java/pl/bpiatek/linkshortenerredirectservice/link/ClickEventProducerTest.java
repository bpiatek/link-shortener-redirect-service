package pl.bpiatek.linkshortenerredirectservice.link;

import jakarta.servlet.http.HttpServletRequest;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.springframework.kafka.core.KafkaTemplate;
import pl.bpiatek.contracts.link.LinkClickEventProto.LinkClickEvent;

import java.util.concurrent.CompletableFuture;

import static java.nio.charset.StandardCharsets.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.quality.Strictness.LENIENT;

@MockitoSettings(strictness = LENIENT)
@ExtendWith(MockitoExtension.class)
class ClickEventProducerTest {

    private static final String TEST_TOPIC = "test-link-clicks";

    @Mock
    private KafkaTemplate<String, LinkClickEvent> kafkaTemplate;

    @Mock
    private HttpServletRequest httpServletRequest;

    @Captor
    private ArgumentCaptor<ProducerRecord<String, LinkClickEvent>> producerRecordCaptor;

    private ClickEventProducer clickEventProducer;

    @BeforeEach
    void setUp() {
        clickEventProducer = new ClickEventProducer(kafkaTemplate, TEST_TOPIC);
    }

    @Test
    void shouldBuildAndSendCorrectClickEvent() {
        // given
        var shortCode = "aB5xZ1";
        var userAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)";
        var ipAddress = "123.123.123.123";

        given(httpServletRequest.getHeader("User-Agent")).willReturn(userAgent);
        given(httpServletRequest.getHeader("User-Agent")).willReturn(userAgent);
        given(httpServletRequest.getHeader("X-FORWARDED-FOR")).willReturn(null);
        given(httpServletRequest.getRemoteAddr()).willReturn(ipAddress);
        given(kafkaTemplate.send(any(ProducerRecord.class)))
                .willReturn(CompletableFuture.completedFuture(null));

        // when
        clickEventProducer.sendClickEvent(shortCode, httpServletRequest);

        // then
        verify(kafkaTemplate).send(producerRecordCaptor.capture());
        var capturedRecord = producerRecordCaptor.getValue();

        assertSoftly(s -> {
            s.assertThat(capturedRecord.topic()).isEqualTo(TEST_TOPIC);
            s.assertThat(capturedRecord.key()).isEqualTo(shortCode);

            var event = capturedRecord.value();
            s.assertThat(event.getShortUrl()).isEqualTo(shortCode);
            s.assertThat(event.getIpAddress()).isEqualTo(ipAddress);
            s.assertThat(event.getUserAgent()).isEqualTo(userAgent);
            s.assertThat(event.hasClickedAt()).isFalse();
            s.assertThat(new String(capturedRecord.headers().lastHeader("source").value(), UTF_8))
                    .isEqualTo("redirect-service");
            s.assertThat(capturedRecord.headers().lastHeader("trace-id")).isNotNull();
        });
    }

    @Test
    void shouldUseXForwardedForHeaderWhenPresent() {
        // given:
        var shortCode = "proxy123";
        var realIp = "200.200.200.200";
        var proxyIp = "10.0.0.1";

        given(httpServletRequest.getHeader("X-FORWARDED-FOR")).willReturn(realIp);
        given(httpServletRequest.getRemoteAddr()).willReturn(proxyIp);
        given(httpServletRequest.getHeader("User-Agent")).willReturn("test-agent");
        given(kafkaTemplate.send(any(ProducerRecord.class)))
                .willReturn(CompletableFuture.completedFuture(null));

        // when
        clickEventProducer.sendClickEvent(shortCode, httpServletRequest);

        // then
        verify(kafkaTemplate, timeout(1000)).send(producerRecordCaptor.capture());
        var capturedEvent = producerRecordCaptor.getValue().value();
        assertThat(capturedEvent.getIpAddress()).isEqualTo(realIp);
    }
}