package org.seto.commons.executor.sequentialexecutor;

import lombok.extern.slf4j.Slf4j;
import org.seto.commons.executor.error.SequentialExecutorError;
import org.springframework.util.concurrent.FailureCallback;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.util.concurrent.SuccessCallback;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by $3T0 Corp<br/>
 * User: devid<br/>
 * Date and Time: 3/2/2021 2:35 PM<br/>
 * <p> Description of SequentialTask </p>
 */
@Slf4j
public class SequentialTask implements ListenableFuture<Object> {
    private static final String SEQUENTIAL_EXECUTOR = "sequential-executor.{0}.task.{1}";
    private static final String CREATED = "created";
    private static final String STARTED = "started";
    private static final String SUCCEEDED = "succeeded";
    private static final String FAILED = "failed";
    private static final int AGE_MAX_SEC = 75;
    private static final int EXECUTION_MAX_SEC = 65;
    private static final int INTERRUPT_SEC = 45;
    private static final int WARNING_SEC = 25;
    private String key;
    private Runnable task;
    private String executorName;
    private volatile Thread workerThread;
    private long createTimeNs;
    private long startTimeNs;
    private final List<ListenableFutureCallback<Object>> callbacks;
    private volatile AtomicBoolean isDone = new AtomicBoolean(false);
    private volatile Throwable executionFailed;

    public SequentialTask(String key, Runnable task, String executorName) {
        createTimeNs = System.nanoTime();
        this.key = key;
        this.task = task;
        this.executorName = executorName;
        callbacks = new CopyOnWriteArrayList<>();
    }

    public String getKey() {
        return key;
    }

    public void execute() {
        if (null != workerThread) {
            log.error("thread:{} already working on this task with key:{}", workerThread.getName(), key);
            return;
        }
        workerThread = Thread.currentThread();
        try {
            startTimeNs = System.nanoTime();
            long ageSec = TimeUnit.NANOSECONDS.toSeconds(startTimeNs - createTimeNs);
            if (ageSec < AGE_MAX_SEC) {
                task.run();
            } else {
                log.error("dropping task aged {} sec with key {}", ageSec, key);
            }
        } catch (Throwable ex) {
            this.executionFailed = ex;
            log.error(String.format("execution failed key:%s, executor:%s", key, executorName), ex);
        }
        setIsDone();
    }

    /**
     * If as stuck thread is detected completes a task's future so that client may restart app
     *
     * @return true if this task passes audit
     */
    public boolean audit() {
        if (isDone()) {
            log.debug("audit: task is already completed by thread:{} key:{}", workerThread.getName(), key);
            return true;
        }
        String workerThreadName = (null != workerThread) ? workerThread.getName() : null;
        long currentTimeNs = System.nanoTime();
        long ageSec = TimeUnit.NANOSECONDS.toSeconds(currentTimeNs - startTimeNs);
        if (ageSec > WARNING_SEC) {
            if (null != workerThread && 0 != startTimeNs) {
                long executionSec = TimeUnit.NANOSECONDS.toSeconds(currentTimeNs - startTimeNs);
                if (executionSec > EXECUTION_MAX_SEC) {
                    log.error("audit: stuck thread:{} executing key:{} for {} sec", workerThreadName, key, executionSec);
                    StringBuilder stringBuilder = new StringBuilder();
                    for (StackTraceElement element : workerThread.getStackTrace()) {
                        stringBuilder.append(element).append(",");
                    }
                    log.error("audit: stuck thread:{} stack-track:{} ...FATAL", workerThreadName, stringBuilder);
                    executionFailed = new SequentialExecutorError(key + " casuing stuck worker thread " + workerThreadName);
                    setIsDone();
                } else if (executionSec > INTERRUPT_SEC) {
                    log.warn("audit: key:{} interrupting:{} age:{} sec execution:{} sec", key, workerThreadName, ageSec, executionSec);
                } else {
                    log.warn("audit: key:{} old-age:{} sec execution:{} sec", key, ageSec, executionSec);
                }
            } else {
                log.warn("audit: no thread executing for key:{} for {} sec started at {} sec", key, ageSec, startTimeNs);
            }
            return false;
        }
        log.debug("audit: key:{} workerThreadName:{} young-age:{} sec", key, workerThreadName, ageSec);
        return true;
    }

    public void completeExceptoinally(RejectedExecutionException rejectedExecutionException) {
        executionFailed = rejectedExecutionException;
        setIsDone();
    }

    @Override
    public CompletableFuture<Object> completable() {
        return null;
    }

    @Override
    public void addCallback(ListenableFutureCallback<? super Object> callback) {
        if (null != callback) {
            //Flow must be synchronized with setIsDone to prevent modifying 'callbacks' while worker thread is iterating
            synchronized (isDone) {
                if (!isDone.get()) {
                    //cache for future notifications
                    callbacks.add(callback);
                    return;
                }
            }
            if (null != executionFailed) {
                callback.onFailure(executionFailed);
            } else {
                callback.onSuccess(this);
            }
        } else {
            log.error("addCallback: null on {}", this);
        }
    }

    @Override
    public void addCallback(SuccessCallback<? super Object> successCallback, FailureCallback failureCallback) {
        if (null != successCallback || null != failureCallback) {
            addCallback(new ListenableFutureCallback<Object>() {

                @Override
                public void onFailure(Throwable throwable) {
                    if (null != failureCallback) {
                        failureCallback.onFailure(throwable);
                    }
                }

                @Override
                public void onSuccess(Object result) {
                    if (null != successCallback) {
                        successCallback.onSuccess(result);
                    }
                }
            });
        } else {
            log.error("addCallback: both successCallback and failureCallback null on {}", this);
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return isDone.get();
    }

    private void setIsDone() {
        try {
            synchronized (isDone) {
                isDone.set(true);
            }
            for (ListenableFutureCallback<Object> callback : callbacks) {
                if (callback != null) {
                    if (null != executionFailed) {
                        callback.onFailure(executionFailed);
                    } else {
                        callback.onSuccess(task);
                    }
                } else {
                    //synchronization in method 'addcallback' failed
                    log.error("setIsDone: null callback of size:{} for key:{}", callbacks.size(), key);
                }
            }
        } catch (Exception e) {
            log.error("setIsDone: failed on key:{}", key, e);
        }
    }

    @Override
    public Object get() throws InterruptedException, ExecutionException {
        while (!isDone()) {
            synchronized (isDone) {
                isDone.wait();
            }
        }
        return this;
    }

    @Override
    public Object get(long timeout, TimeUnit unit) throws
            InterruptedException, ExecutionException, TimeoutException {
        while (!isDone()) {
            synchronized (isDone) {
                isDone.wait(unit.toMillis(timeout));
            }
        }
        return this;
    }
}
