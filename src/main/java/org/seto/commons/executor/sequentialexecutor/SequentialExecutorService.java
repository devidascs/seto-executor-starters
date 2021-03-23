package org.seto.commons.executor.sequentialexecutor;

import org.seto.commons.executor.batch.ExecutorBatch;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public interface SequentialExecutorService {
    /**
     * used a batch to execute a set of tasks
     *
     * @return
     */
    ExecutorBatch createBatch();

    /**
     * Main API of the sequential-executor
     *
     * @param key: tasks with the same key are guaranteed to be executed sequentially, in the order they were applied
     * @param task :  task to be executed
     * @return future which complete when execution of the task completes
     */
    ListenableFuture<Object> execute(String key, Runnable task);

    /**
     * Main API of the sequential-executor
     *
     * @param key:           tasks with the same key are guaranteed to be executed sequentially, in the order they were applied
     * @param task           :  task to be executed
     * @param submitTimeout: value 0 means that there will be no retry if queue is full. Default is Long.MAX_VALUE
     * @param timeUnit
     * @return future which complete when execution of the task completes
     */
    default ListenableFuture<Object> execute(String key, Runnable task, long submitTimeout, TimeUnit timeUnit) {
        return execute(key, task);
    }

    /**
     * use a std executor where ordering doesn't matter
     *
     * @param task
     * @return
     */
    ListenableFuture<Object> execute(Runnable task);

    /**
     * Schedule a task top be executed in the future using the key to control the sequencing
     *
     * @param key   to be used when the execution is injected. May be null
     * @param task
     * @param delay before the execution of the task begins
     * @param unit
     * @return
     */
    ScheduledFuture<ListenableFuture<Object>> schedule(String key, Runnable task, int delay, TimeUnit unit);

    /**
     * Wait until the executor has completed processing all tasks
     * Only then should the messages by committed as consumed
     *
     * @param timeout maximum time to wait
     * @param unit
     * @return: true if all tasks were completed before timeout
     */
    boolean waitUntilComplete(long timeout, TimeUnit unit);

    /**
     * no new tasks will be executed, threads will end
     */
    void shutDown();

    /**
     * @return true if this executor has been shut down
     */
    boolean isShutDown();
}
