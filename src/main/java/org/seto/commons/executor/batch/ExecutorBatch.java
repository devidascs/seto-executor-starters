package org.seto.commons.executor.batch;

import org.springframework.util.concurrent.ListenableFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface ExecutorBatch extends ListenableFuture<Integer> {
    /**
     * Main API of the ExecutorBatch
     *
     * @param key: tasks with the same key are guaranteed to be executed sequentially, in the order they were applied
     * @param task :  task to be executed
     * @return future which complete when execution of the task completes
     */
    ListenableFuture<Object> execute(String key, Runnable task);
    /**
     * use a std executor where ordering doesn't matter
     *
     * @param task
     * @return
     */
    ListenableFuture<Object> execute(Runnable task);

    /**
     * Main API of the ExecutorBatch
     *
     * @param key:           tasks with the same key are guaranteed to be executed sequentially, in the order they were applied
     * @param task           :  task to be executed
     * @param submitTimeout: value 0 means that there will be no retry if queue is full. Default is Long.MAX_VALUE
     * @param submitTimeoutUnit
     * @return future which complete when execution of the task completes
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
    ExecutorBatch setBatchThrottleFreeDuration(int duration, TimeUnit unit);

    /**
     * @param timeout max time to wait for the execution of all tasks in batch to complete
     * @param unit
     * @return the number of incomplete tasks on reaching timeout
     */
    int waitForComplete(long timeout, TimeUnit unit) throws TimeoutException, InterruptedException, ExecutionException;

    /**
     * @return the amount of time in ms since the batch was created
     */
    long getElapsedTimeMS();
}
