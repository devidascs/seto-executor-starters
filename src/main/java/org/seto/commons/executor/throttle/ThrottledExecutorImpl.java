package org.seto.commons.executor.throttle;

import lombok.extern.slf4j.Slf4j;
import org.seto.commons.executor.sequentialexecutor.SequentialExecutorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Created by $3T0 Corp<br/>
 * User: devid<br/>
 * Date and Time: 3/1/2021 11:47 AM<br/>
 * <p>
 * Guarantees that no more than one execution of a given operation will tkae place in the throttling window.
 * The first operation withing the window is executed immediately in the context of the client thread.
 * The last operation will be executed in the context of the SequentialExecutorService thread
 * </p>
 *
 * <p>
 * If for a given operation a second and third operation (with the same key) is received within the throttling window
 * then the second operation will never be executed and the third operation will only be executed when the throttling
 * interval expires and it is the last requested requested operation
 * </p>
 */
@Component
@Slf4j
public class ThrottledExecutorImpl<T> implements ThrottledExecutor<T> {
    private int auditOperationFrequency = 500;
    private long operationCount;
    private final SequentialExecutorService executor;
    private final Map<String, Operation<T>> throttleMap = new HashMap<>();
    private final Operation<T> emptyOperation = new Operation<>(null, null, -1L);

    @Autowired
    public ThrottledExecutorImpl(SequentialExecutorService executor) {
        this.executor = executor;
    }

    class Operation<E> {
        Consumer<E> method;
        E arguement;
        long throttleIntervalMs;

        public Operation(Consumer<E> method, E arguement, long throttleIntervalMs) {
            this.method = method;
            this.arguement = arguement;
            this.throttleIntervalMs = throttleIntervalMs;
        }
    }

    /**
     * @param consumer     method to be executed
     * @param event        parameter of the method
     * @param key          operation id
     * @param throttleTime amount of time to elapse before a throttle operation is executed
     * @param throttleUnit
     */
    public void throttlExecution(Consumer<T> consumer, T event, String key, int throttleTime, TimeUnit throttleUnit) {
        if (null != key) {
            operationCount++;
            if (operationCount % auditOperationFrequency == 0) {
                log.info("throttleExecution key:{} currentOperationCount:{} totalcount:{}", key, getThrottleCount(), operationCount);
            }
            synchronized (throttleMap) {
                Operation<T> lastOp = throttleMap.get(key);
                if (null != throttleMap) {
                    throttleMap.put(key, new Operation<>(consumer, event, throttleUnit.toMillis(throttleTime)));
                    //second or later request within the throttling internal required only an update of the map
                    return;
                }
                //First request withing the throttling interval. Execute now but leave an indicator that future
                // operation withing the throttle time must be throttled
                throttleMap.put(key, emptyOperation);
            }
            executor.schedule(key, () -> onThrottlingIntervalElapsed(key), throttleTime, throttleUnit);
        }
        log.info("throttlExecution: callback now for key:{}", key);
        consumer.accept(event);
    }

    /**
     * Executes the operation now
     *
     * @param key operation identifier
     */
    protected void onThrottlingIntervalElapsed(String key) {
        Operation<T> throttledOperation;
        synchronized (throttleMap) {
            throttledOperation = throttleMap.get(key);
            if (null != throttledOperation && throttledOperation.method != null) {
                //Executing now so throttle for another interval
                throttleMap.put(key, emptyOperation);
            } else if (null != throttledOperation) {
                throttleMap.remove(key);
            }
        }
        if (null != throttledOperation && null != throttledOperation.method) {
            log.debug("onThrottlingIntervalElapsed: callback now for key:{}", key);
            //Avoid leaking 'emptyOperation' in map for each key
            executor.schedule(key, () -> onThrottlingIntervalElapsed(key), (int) throttledOperation.throttleIntervalMs, TimeUnit.MILLISECONDS);
            throttledOperation.method.accept(throttledOperation.arguement);
        } else if (throttledOperation != null) {
            log.debug("onThrottlingIntervalElapsed: no new executions requested during throttling intervals for key:{}", key);
        } else {
            log.warn("onThrottlingIntervalElapsed: operation not found for key:{}", key);
        }
    }

    /**
     * return the no of executions being throttled
     *
     * @return
     */
    public int getThrottleCount() {
        synchronized (throttleMap) {
            int size = throttleMap.size();
            return size;
        }

    }
}
