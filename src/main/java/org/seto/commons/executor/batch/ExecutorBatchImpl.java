package org.seto.commons.executor.batch;

import lombok.extern.slf4j.Slf4j;
import org.seto.commons.executor.sequentialexecutor.SequentialExecutorService;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by $3T0 Corp<br/>
 * User: devid<br/>
 * Date and Time: 3/4/2021 5:26 PM<br/>
 * <p> ExecutorBatch
 * Use the executor to execute tasks from one or more batches and use this Future's get API to wait for completion of this batch.
 * Execution begins immediately, does not wait for all tasks to be added to the batch.
 * Batch level throttling may be applied so that low priority tasks are not dropped unnecessarily.</p>
 */
@Slf4j
public class ExecutorBatchImpl extends CompoundFuture implements ExecutorBatch {
    private final SequentialExecutorService executor;
    private final String id;
    private String name;
    private long batchSizeExpected = 10;
    private long batchCreateTimeMs;
    private long batchThrottleFreeSubmitTimeMs = 50;
    private boolean batchThrottleFreeSumbitTimeReached = false;

    public ExecutorBatchImpl(SequentialExecutorService executor) {
        this.executor = executor;
        this.id = UUID.randomUUID().toString();
        this.batchCreateTimeMs = System.currentTimeMillis();
    }

    @Override
    public ListenableFuture<Object> execute(String key, Runnable task) {
        ListenableFuture<Object> future = executor.execute(key, task);
        super.add(future);
        return future;
    }

    @Override
    public ListenableFuture<Object> execute(Runnable task) {
        ListenableFuture<Object> future = executor.execute(task);
        super.add(future);
        return future;
    }

    @Override
    public ListenableFuture<Object> execute(String key, Runnable task, long submitTimeout, TimeUnit submitTimeoutUnit) {
        long submitTimeoutMs = submitTimeoutUnit.toMillis(submitTimeout);
        if (!batchThrottleFreeSumbitTimeReached) {
            long remainingCount = batchSizeExpected - super.size();
            long remainingThrottleFreeTimeMs = batchThrottleFreeSubmitTimeMs - getElapsedTimeMS();
            if (remainingThrottleFreeTimeMs <= 0) {
                batchThrottleFreeSumbitTimeReached = true;
                log.info("Throttle started on {}", this);
            } else if (remainingCount > 0) {
                //Increase submitTimeout to avoid unnecessary throttling, but don't allow one task to hog all the allowable time
                long remainingThrottleFreeTimePerTask = remainingThrottleFreeTimeMs / remainingCount;
                if (remainingCount > submitTimeoutMs) {
                    submitTimeoutMs = remainingThrottleFreeTimePerTask;
                    log.debug("Submit timeout modified to {} remainingThrottleFreeTimePerTask:{} " +
                            "remainingCount", submitTimeoutMs, remainingThrottleFreeTimeMs, remainingCount);
                }
            }
        }
        ListenableFuture<Object> future = executor.execute(key, task, submitTimeoutMs, TimeUnit.MILLISECONDS);
        super.add(future);
        return future;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public ExecutorBatch setName(String name) {
        this.name = name;
        return this;
    }

    @Override
    public ExecutorBatch setBatchSize(long batchSizeExpected) {
        this.batchSizeExpected = batchSizeExpected > 0 ? batchSizeExpected : 1;
        return this;
    }

    @Override
    public ExecutorBatch setThrottleFreeDuration(int duration, TimeUnit unit) {
        if (duration > 0) {
            this.batchThrottleFreeSubmitTimeMs = unit.toMillis(duration);
        }
        return this;
    }

    @Override
    /**
     * Wait for completion while logging progress
     * @return the number of remaining tasks. Zero for success
     * @throws InterruptedException
     * @throws java.util.concurrent.ExecutionException Any task that throws @{@link Error} unblokcs the wait and
     * the client may chose to terminate the app
     *
     */
    public int waitForComplete(long timeout, TimeUnit unit) {
        long timeoutMs = unit.toMillis(timeout);
        long timeoutSnapshotMS = timeoutMs / 4;
        int incompleteTaskCount = 1;
        int waitIntervals = 0;
        while (incompleteTaskCount > 0 && ++waitIntervals < 4) {
            try {
                incompleteTaskCount = get(timeoutSnapshotMS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.warn("waitForComplete: InterruptedException remaining:{} on {}", incompleteTaskCount, this);
            } catch (ExecutionException e) {
                log.warn("waitForComplete: ExecutionException remaining:{} on {}", incompleteTaskCount, this);
            } catch (TimeoutException e) {
                log.warn("waitForComplete: TimeoutException remaining:{} on {}", incompleteTaskCount, this);
            }
            if (incompleteTaskCount > 0) {
                log.info("waited:{} ms to complete {} tasks in batch:{} executor:{}",
                        waitIntervals + timeoutSnapshotMS, incompleteTaskCount, this, this.executor);
            }
            if (executor.isShutDown()) {
                log.warn("Stopping unreliably with {} incomplete tasks on:{}", incompleteTaskCount, this);
            }
        }
        if (incompleteTaskCount > 0) {
            log.warn("waited:{} ms but failed to complete {} tasks in batch:{} executor:{}",
                    waitIntervals + timeoutSnapshotMS, incompleteTaskCount, this, this.executor);
        }
        return incompleteTaskCount;
    }

    @Override
    public long getElapsedTimeMS() {
        return System.currentTimeMillis() - batchCreateTimeMs;
    }

    @Override
    public String toString() {
        return "ExecutorBatchImpl{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", batchSizeExpected=" + batchSizeExpected +
                '}';
    }
}
