package pl.bpiatek.linkshortenerredirectservice.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import io.micrometer.observation.ObservationRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import pl.bpiatek.contracts.link.LinkClickEventProto.LinkClickEvent;
import pl.bpiatek.contracts.link.LinkLifecycleEventProto.LinkLifecycleEvent;

import java.util.Map;

@Configuration
@EnableKafka
class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    private final KafkaProperties kafkaProperties;

    public KafkaConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public ProducerFactory<String, LinkClickEvent> linkClickEventProducerFactory() {
        Map<String, Object> props = kafkaProperties.buildProducerProperties(null);

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);

        props.putIfAbsent(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        props.putIfAbsent(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, true);
        putSchemaRegistryUrl(props);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, LinkClickEvent> linkClickEventKafkaTemplate(ObservationRegistry observationRegistry) {
        var template = new KafkaTemplate<>(linkClickEventProducerFactory());

        template.setObservationEnabled(true);
        template.setObservationRegistry(observationRegistry);

        return template;
    }

    @Bean
    public ConsumerFactory<String, LinkLifecycleEvent> linkLifecycleEventConsumerFactory() {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
        props.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, LinkLifecycleEvent.class);
        putSchemaRegistryUrl(props);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    @Primary
    public ConcurrentKafkaListenerContainerFactory<String, LinkLifecycleEvent> linkLifecycleEventsContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<String, LinkLifecycleEvent> linkLifecycleEventConsumerFactory) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, LinkLifecycleEvent>();

        configurer.configure(
                (ConcurrentKafkaListenerContainerFactory) factory,
                (ConsumerFactory) linkLifecycleEventConsumerFactory
        );

        return factory;
    }

    private void putSchemaRegistryUrl(Map<String, Object> props) {
        var registryUrl = kafkaProperties.getProperties().get("schema.registry.url");
        if (registryUrl != null) {
            props.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        }
    }
}
