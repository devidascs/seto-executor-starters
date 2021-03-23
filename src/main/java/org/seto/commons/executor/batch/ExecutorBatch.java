package org.seto.commons.executor.batch;

import org.springframework.util.concurrent.ListenableFuture;

import java.util.concurrent.TimeUnit;

public interface ExecutorBatch {
    /**
     * @param key
     * @param task
     * @return
     */
    ListenableFuture<Object> execute(String key, Runnable task);

    /**
     * @param task
     * @return
     */
    ListenableFuture<Object> execute(Runnable task);

    /**
     * @param key
     * @param task
     * @param submitTimeout
     * @param submitTimeoutUnit
     * @return
     */
    default ListenableFuture<Object> execute(
            String key, Runnable task, long submitTimeout, TimeUnit submitTimeoutUnit) {
        return execute(key, task);
    }

    /**
     * @return
     */
    String getId();

    /**
     * @param name: name of client
     * @return
     */
    ExecutorBatch setName(String name);

    /**
     * Called before first execution to fine-tune throttling
     *
     * @param batchSizeExpected
     * @return
     */
    ExecutorBatch setBatchSize(long batchSizeExpected);

    /**
     * Avoid unnecessary throttling by specifying a time during which more throttling than requested in the tasks is applied
     * E.g. a large number of messages with low throttling would be dropped if injected faster than worked threads can start
     *
     * @param duration
     * @param unit
     * @return
     */
    ExecutorBatch setThrottleFreeDuration(int duration, TimeUnit unit);

    /**
     * @param timeout max time to wait for the execution of all tasks in batch to complete
     * @param unit
     * @return the number of incomplete tasks on reaching timeout
     */
    int waitForComplete(long timeout, TimeUnit unit);

    /**
     * @return the amount of time in ms since the batch was created
     */
    long getElapsedTimeMS();
}
