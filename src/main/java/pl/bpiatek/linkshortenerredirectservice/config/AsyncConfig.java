package pl.bpiatek.linkshortenerredirectservice.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;

@Configuration
@EnableAsync
class AsyncConfig implements AsyncConfigurer {

    private final TaskExecutor virtualThreadExecutor;

    AsyncConfig(@Qualifier("virtualThreadExecutor") TaskExecutor virtualThreadExecutor) {
        this.virtualThreadExecutor = virtualThreadExecutor;
    }

    @Override
    public TaskExecutor getAsyncExecutor() {
        return this.virtualThreadExecutor;
    }
}