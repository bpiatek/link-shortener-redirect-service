package pl.bpiatek.linkshortenerredirectservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;

@Configuration
public class VirtualThreadConfig {

    @Bean(name = "virtualThreadExecutor")
    public TaskExecutor virtualThreadTaskExecutor() {
        return runnable -> Thread.ofVirtual().start(runnable);
    }
}