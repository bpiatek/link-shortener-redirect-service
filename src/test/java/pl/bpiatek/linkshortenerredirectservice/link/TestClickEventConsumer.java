package pl.bpiatek.linkshortenerredirectservice.link;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import pl.bpiatek.contracts.link.LinkClickEventProto.LinkClickEvent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Component
@Profile("test")
public class TestClickEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(TestClickEventConsumer.class);

    private CountDownLatch latch = new CountDownLatch(1);
    private ConsumerRecord<String, LinkClickEvent> payload;

    @KafkaListener(
            topics = "${topic.link.clicks}",
            groupId = "test-click-event-consumer-group"
    )
    public void receive(ConsumerRecord<String, LinkClickEvent> consumerRecord) {
        log.info("Test consumer received click event with key: {}", consumerRecord.key());
        payload = consumerRecord;
        latch.countDown();
    }

    public ConsumerRecord<String, LinkClickEvent> awaitRecord(long timeout, TimeUnit unit) throws InterruptedException {
        if (!latch.await(timeout, unit)) {
            throw new IllegalStateException("No click event message received in the allotted time");
        }
        return payload;
    }

    public void reset() {
        latch = new CountDownLatch(1);
        payload = null;
    }
}