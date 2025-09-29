package pl.bpiatek.linkshortenerredirectservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;

@Configuration
class VirtualThreadConfig {

    @Bean(name = "virtualThreadExecutor")
    TaskExecutor virtualThreadTaskExecutor() {
        return runnable -> Thread.ofVirtual().name("virtual-", 0).start(runnable);
    }
}