package org.seto.commons.executor.batch;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.concurrent.FailureCallback;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.util.concurrent.SuccessCallback;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by $3T0 Corp<br/>
 * User: devid<br/>
 * Date and Time: 3/22/2021 10:32 AM<br/>
 * <p> Description of CompoundFuture </p>
 */
@Slf4j
public class CompoundFuture implements ListenableFuture<Integer> {
    public static final int LOG_WAIT_THROTTLE = 10;
    private final AtomicInteger addedCount = new AtomicInteger();
    private final AtomicInteger inProgressCount = new AtomicInteger();
    private final InProgressCounter inProgressCounter = new InProgressCounter();
    SuccessCallback<? super Integer> successCallback;
    FailureCallback failureCallback;

    public void add(ListenableFuture<? extends Object> future) {
        inProgressCounter.add(future);
        addedCount.incrementAndGet();
    }

    @Override
    public CompletableFuture<Integer> completable() {
        return null;
    }

    @Override
    public void addCallback(ListenableFutureCallback<? super Integer> listenableFutureCallback) {
        this.successCallback = listenableFutureCallback;
        this.failureCallback = listenableFutureCallback;
    }

    @Override
    public void addCallback(SuccessCallback<? super Integer> successCallback, FailureCallback failureCallback) {
        this.successCallback = successCallback;
        this.failureCallback = failureCallback;
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
        return inProgressCount.get() == 0;
    }

    @SneakyThrows
    @Override
    public Integer get() throws InterruptedException, ExecutionException {
        return get(1, TimeUnit.DAYS);
    }

    @Override
    public Integer get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        long startTimeNano = System.nanoTime();
        long remainingTimeNano = unit.toNanos(timeout);
        final long maxTimeNano = remainingTimeNano + startTimeNano;
        synchronized (inProgressCount) {
            int loops = 0;
            while (inProgressCount.get() > 0 && remainingTimeNano > 0) {
                inProgressCount.wait(10);
                remainingTimeNano = maxTimeNano - System.nanoTime();
                if (++loops == LOG_WAIT_THROTTLE) {
                    log.debug("get: still waiting for {} tasks to complete", inProgressCount);
                    loops = 0;
                }
            }
        }
        Integer remainingCount = Integer.valueOf(inProgressCount.get());
        if (remainingCount == 0) {
            log.info("post-sumbit wait-time {} ms batch-size:{}",
                    TimeUnit.NANOSECONDS.toMillis(unit.toNanos(timeout) - remainingTimeNano), addedCount);
        }
        return remainingCount;
    }

    /**
     * returns number for futures added
     *
     * @return
     */
    public int size() {
        return addedCount.get();
    }

    private class InProgressCounter implements ListenableFutureCallback<Object> {
        @Override
        public void onFailure(Throwable throwable) {
            //If a task fails with a FATAL ERROR then this CompondFuture is immediately considered a failure
            if (throwable instanceof Error && null != failureCallback) {
                inProgressCount.decrementAndGet();
                failureCallback.onFailure(throwable);
            } else {
                //If a task fails with a std exception then normal processing continues and this CompondFuture is not a failure
                onTaskComplete();
            }
        }

        @Override
        public void onSuccess(Object result) {
            onTaskComplete();
        }

        private void onTaskComplete() {
            if (inProgressCount.decrementAndGet() == 0) {
                synchronized (inProgressCount) {
                    inProgressCount.notifyAll();
                }
                if (null != successCallback) {
                    successCallback.onSuccess(0);
                }
            }
        }

        public void add(ListenableFuture<? extends Object> future) {
            inProgressCount.incrementAndGet();
            future.addCallback(this);
        }
    }
}
