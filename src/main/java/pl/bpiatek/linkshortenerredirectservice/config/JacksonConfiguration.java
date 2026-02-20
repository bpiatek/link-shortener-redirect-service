package pl.bpiatek.linkshortenerredirectservice.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
class JacksonConfiguration {

    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        return JsonMapper.builder()
                // Support for Java 8+ Date/Time (Instant, LocalDateTime)
                .addModule(new JavaTimeModule())
                // Support for Records and Parameter Names
                .addModule(new ParameterNamesModule())
                // Ignore properties in JSON that aren't in your Java class
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                // Write dates as ISO-8601 strings, not timestamps
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .build();
    }
}
