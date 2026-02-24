package pl.bpiatek.linkshortenerredirectservice.link;

import com.google.protobuf.Timestamp;
import io.micrometer.context.ContextSnapshotFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import pl.bpiatek.contracts.link.LinkClickEventProto.LinkClickEvent;

import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.slf4j.LoggerFactory.*;

@Service
public class ClickEventPublisher {

    private static final Logger log = getLogger(ClickEventPublisher.class);
    private static final ContextSnapshotFactory snapshotFactory = ContextSnapshotFactory.builder().build();
    private static final String SOURCE_HEADER_VALUE = "redirect-service";

    private final KafkaTemplate<String, LinkClickEvent> kafkaTemplate;
    private final String topicName;
    private final Clock clock;

    private final ExecutorService analyticsExecutor = Executors.newVirtualThreadPerTaskExecutor();

    ClickEventPublisher(KafkaTemplate<String, LinkClickEvent> kafkaTemplate,
                        @Value("${topic.link.clicks}") String topicName,
                        Clock clock) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicName = topicName;
        this.clock = clock;
    }

    public void publishSafe(String shortUrl, String ipAddress, String userAgent) {
        //  Capture the trace/MDC context from the Tomcat/Web thread
        var snapshot = snapshotFactory.captureAll();

        //  Offload to a Virtual Thread to prevent Kafka metadata timeouts from blocking the 302 redirect
        analyticsExecutor.submit(() -> {
            try (var scope = snapshot.setThreadLocals()) {
                doSendClickEvent(shortUrl, ipAddress, userAgent);
            }
        });
    }

    void doSendClickEvent(String shortUrl, String ipAddress, String userAgent) {
        var now = clock.instant();
        var eventId = UUID.randomUUID().toString();

        var event = LinkClickEvent.newBuilder()
                .setShortUrl(shortUrl)
                .setIpAddress(ipAddress != null ? ipAddress : "UNKNOWN")
                .setUserAgent(userAgent != null ? userAgent : "UNKNOWN")
                .setClickedAt(Timestamp.newBuilder()
                        .setSeconds(now.getEpochSecond())
                        .setNanos(now.getNano())
                        .build())
                .build();

        var producerRecord = new ProducerRecord<>(topicName, shortUrl, event);
        producerRecord.headers().add(new RecordHeader("source", SOURCE_HEADER_VALUE.getBytes(UTF_8)));
        producerRecord.headers().add(new RecordHeader("event-id", eventId.getBytes(UTF_8)));


        kafkaTemplate.send(producerRecord).whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Successfully published LinkClickEvent for shortCode: {} with eventId: {}", shortUrl, eventId);
            } else {
                log.error("Failed to publish LinkClickEvent for shortCode: {}. Reason: {}", shortUrl, ex.getMessage());
            }
        });
    }
}
