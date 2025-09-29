package pl.bpiatek.linkshortenerredirectservice.link;

import com.google.protobuf.Timestamp;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import pl.bpiatek.contracts.link.LinkClickEventProto.LinkClickEvent;

import java.time.Clock;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;

@Service
public class ClickEventProducer {

    private static final Logger log = LoggerFactory.getLogger(ClickEventProducer.class);
    private static final String SOURCE_HEADER_VALUE = "redirect-service";

    private final KafkaTemplate<String, LinkClickEvent> kafkaTemplate;
    private final String topicName;
    private final Clock clock;

    ClickEventProducer(KafkaTemplate<String, LinkClickEvent> kafkaTemplate,
                       @Value("${topic.link.clicks}") String topicName,
                       Clock clock) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicName = topicName;
        this.clock = clock;
    }

    public void sendClickEvent(String shortUrl, HttpServletRequest request) {
        var ip = getClientIp(request);
        var userAgent = request.getHeader("User-Agent");

        doSendClickEvent(shortUrl, ip, userAgent);
    }

    @Async
    protected void doSendClickEvent(String shortUrl, String ipAddress, String userAgent) {
        var now = clock.instant();

        var eventToSend = LinkClickEvent.newBuilder()
                .setShortUrl(shortUrl)
                .setIpAddress(ipAddress)
                .setUserAgent(userAgent)
                .setClickedAt(Timestamp.newBuilder()
                        .setSeconds(now.getEpochSecond())
                        .setNanos(now.getNano())
                        .build())
                .build();

        var producerRecord = new ProducerRecord<>(topicName, shortUrl, eventToSend);

        producerRecord.headers().add(new RecordHeader("trace-id", UUID.randomUUID().toString().getBytes(UTF_8)));
        producerRecord.headers().add(new RecordHeader("source", SOURCE_HEADER_VALUE.getBytes(UTF_8)));

        kafkaTemplate.send(producerRecord).whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Successfully published LinkClickEvent for shortCode: {}", shortUrl);
            } else {
                log.error("Failed to publish LinkClickEvent for shortCode: {}. Reason: {}", shortUrl, ex.getMessage());
            }
        });
    }

    private String getClientIp(HttpServletRequest request) {
        if (request == null) {
            return "";
        }

        var ipAddress = request.getHeader("CF-Connecting-IP");
        if (ipAddress == null || ipAddress.isEmpty()) {
            ipAddress = request.getRemoteAddr();
        }

        return ipAddress;
    }
}
