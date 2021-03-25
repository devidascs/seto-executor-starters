package org.seto.commons.executor.throttle;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public interface ThrottledExecutor<T> {
    /**
     * @param consumer     method to be executed
     * @param event        parameter of the method
     * @param key          operation id
     * @param throttleTime amount of time to elapse before a throttle operation is execured
     * @param throttleUnit
     */
    void throttlExecution(Consumer<T> consumer, T event, String key, int throttleTime, TimeUnit throttleUnit);

    /**
     * @return no of executions being throttled
     */
    int getThrottleCount();
}
