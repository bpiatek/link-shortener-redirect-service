package pl.bpiatek.linkshortenerredirectservice.config;

import io.prometheus.metrics.tracer.common.SpanContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class PrometheusExemplarFixConfig {

    @Bean
    SpanContext disablePrometheusExemplars() {
        return new SpanContext() {
            @Override
            public String getCurrentTraceId() { return null; }

            @Override
            public String getCurrentSpanId() { return null; }

            @Override
            public boolean isCurrentSpanSampled() { return false; }

            @Override
            public void markCurrentSpanAsExemplar() {}
        };
    }
}