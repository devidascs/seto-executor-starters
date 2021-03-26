package org.seto.commons.executor.sequentialexecutor;

import lombok.extern.slf4j.Slf4j;
import org.seto.commons.executor.batch.ExecutorBatch;
import org.seto.commons.executor.batch.ExecutorBatchImpl;
import org.seto.commons.executor.error.SequentialExecutorException;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by $3T0 Corp<br/>
 * User: devid<br/>
 * Date and Time: 3/1/2021 11:58 AM<br/>
 * <p> Executed unrelated tasks independently and in parallel. Executes related tasks(same key) sequentially and in the older they were applied</p>
 */
@Slf4j
public class SequentialExecutorServiceImpl implements SequentialExecutorService {
    private static final int DEFAULT_MAX_QUEUE_SIZE = 100;
    private static final int INJECTION_THROTTLE_TIME_MS = 50;
    private static final RejectedExecutionException REJECTED_EXECUTION_EXCEPTION = new RejectedExecutionException();
    private final int threadPoolSize;
    private final int maxKeyQueueSize;
    private final int maxMainQueueSize;
    private final AtomicInteger consecutiveFailedCompletes = new AtomicInteger(0);
    private final AtomicInteger suspendedThreadCount = new AtomicInteger(0);
    private final Queue<SequentialTask> taskQueue;
    private final Map<String, Queue<SequentialTask>> taskExecutionMap = new ConcurrentHashMap<>();
    private final Map<Long, SequentialTask> taskByThreadMap = new ConcurrentHashMap<>();
    private final List<Long> threadIds;
    private final ScheduledExecutorService scheduledExec;
    private final ExecutorService backingExecutor;
    private final SequentialExecutor seqExecutor;
    private final String name;
    private final boolean blockOnQueueFull;
    private volatile boolean isRunning = true;
    private final ThreadGroup threadGroup;
    private static final String TASK_QUEUE = "sequential-executor.{0}.taskQueue";
    private static final String TASK_EXECUTION_MAP = "sequential-executor.{0}.taskExecutionMap";
    private static final String TASK_SUBMIT_TIMEOUT = "sequential-executor.{0}.taskSubmitTimeout";

    public SequentialExecutorServiceImpl(String name, int threadPoolSize) {
        this(name, threadPoolSize, DEFAULT_MAX_QUEUE_SIZE, true);
    }

    /**
     *
     * @param name: name of the executor
     * @param threadPoolSize: total number of threads
     * @param maxKeyQueueSize: max queued tasks per key
     * @param blockOnQueueFull: block client thread adding new task if too many tasks are awaiting execution
     */
    public SequentialExecutorServiceImpl(
            String name,
            int threadPoolSize, int maxKeyQueueSize, boolean blockOnQueueFull) {
        log.info("SequentialExecutorServiceImpl() called with: name = [" + name + "], threadPoolSize = [" +
                threadPoolSize + "], maxKeyQueueSize = [" + maxKeyQueueSize + "], blockOnQueueFull = [" +
                blockOnQueueFull + "]");
        this.threadPoolSize = threadPoolSize;
        this.maxKeyQueueSize = maxKeyQueueSize;
        this.maxMainQueueSize = maxKeyQueueSize * 1;
        this.name = name;
        this.blockOnQueueFull = blockOnQueueFull;
        this.taskQueue = new LinkedBlockingDeque<>();
        this.seqExecutor = new SequentialExecutor();
        this.threadGroup = new ThreadGroup(name);

        AtomicInteger threadIndex = new AtomicInteger(0);
        threadIds = new ArrayList<>(threadPoolSize);
        backingExecutor = Executors.newFixedThreadPool(threadPoolSize, r -> {
            Thread t = new Thread(threadGroup, r, name + "-" + threadIndex.incrementAndGet());
            t.setDaemon(true);
            threadIds.add(t.getId());
            return t;
        });

        for (int i = 0; i < threadPoolSize; i++) {
            backingExecutor.execute(seqExecutor);
        }
        scheduledExec = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, name + "-sched"));
        scheduledExec.scheduleWithFixedDelay(this::auditThreads, 1, 1, TimeUnit.MINUTES);
    }

    @Override
    public ScheduledFuture<ListenableFuture<Object>> schedule(String key, Runnable task, int delay, TimeUnit unit) {
        log.debug("{} schedule: key{} delay:{}", name, key, delay);
        if (delay > 0) {
            return scheduledExec.schedule(() -> execute(key, task), delay, unit);
        } else {
            execute(key, task);
            return null;
        }
    }

    @Override
    public ListenableFuture<Object> execute(String key, Runnable task) {
        return executeSequentialTask(new SequentialTask(key, task, name), Long.MAX_VALUE);
    }

    @Override
    public ListenableFuture<Object> execute(Runnable task) {
        return executeSequentialTask(new SequentialTask(null, task, name), Long.MAX_VALUE);
    }

    @Override
    public ListenableFuture<Object> execute(String key, Runnable task, long submitTimeout, TimeUnit timeUnit) {
        return executeSequentialTask(new SequentialTask(key, task, name), timeUnit.toMillis(submitTimeout));
    }

    public ListenableFuture<Object> executeSequentialTask(SequentialTask seqTask, long submitTimeoutMs) {
        long submitTimeRemaining = submitTimeoutMs;
        while (isRunning) {
            synchronized (taskQueue) {
                if (!throttleClientThread(seqTask) && taskQueue.offer(seqTask)) {
                    taskQueue.notify();
                    return seqTask;
                }
                try {
                    long waitTimeMs = submitTimeRemaining < INJECTION_THROTTLE_TIME_MS ? submitTimeRemaining :
                            INJECTION_THROTTLE_TIME_MS;
                    if (waitTimeMs <= 0) {
                        seqTask.completeExceptoinally(REJECTED_EXECUTION_EXCEPTION);
                        log.info("{} rejected key:{} submitTimeout:{} taskQueue.size:{}", name,
                                seqTask.getKey(), submitTimeoutMs, taskQueue.size());
                        return seqTask;
                    }
                    submitTimeRemaining -= waitTimeMs;
                    //Apply back-pressure by blocking the client-thread(too low a value will cause contention with worker threads)
                    taskQueue.wait(waitTimeMs);
                    log.info("{} throttle complete on taskQueue.size:{} on key:{}", name, taskQueue.size(),
                            seqTask.getKey());
                } catch (InterruptedException e) {
                    log.warn("{} client-thread interrupted and quitting", name);
                    Thread.currentThread().interrupt();
                }

            }
        }
        throw new SequentialExecutorException("task rejected because executor was not running key:" + seqTask.getKey());
    }

    private boolean throttleClientThread(SequentialTask seqTask) {
        if (!blockOnQueueFull) {
            return false;
        }
        if (Thread.currentThread().getThreadGroup().equals(this.threadGroup)) {
            log.debug("disable throttle for internal threads on key:{}", seqTask.getKey());
            return false;
        }
        int keyQueueSize = 0;
        if (seqTask.getKey() != null) {
            Queue<SequentialTask> perKeyQueue = taskExecutionMap.get(seqTask.getKey());
            if (perKeyQueue != null) {
                keyQueueSize = perKeyQueue.size();
                if (keyQueueSize >= this.maxKeyQueueSize - 1) {
                    log.warn("{} throttle due to keyQueue[{}].size:{}, taskQueue.size:{} on key:{}", name,
                            perKeyQueue.hashCode(), taskQueue.size(), seqTask.getKey());
                    return true;
                }
            }
        }
        boolean blockClientThread = (taskQueue.size() >= maxMainQueueSize - keyQueueSize);
        if (blockClientThread) {
            log.warn("{} throttle due to taskQueue.size:{} keyQueue.size:{} on key:{}", name, taskQueue.size(),
                    keyQueueSize, seqTask.getKey());
        }
        return blockClientThread;
    }
    public class SequentialExecutor implements Runnable {
        private SequentialTask getNextTask() {
            suspendedThreadCount.incrementAndGet();
            SequentialTask executable = taskQueue.poll();
            while (isRunning && null == executable) {
                try {
                    taskQueue.wait(1000);
                    executable = taskQueue.poll();
                } catch (InterruptedException e) {
                    if (isRunning) {
                        log.warn("{} worker-thread interrupted and not quitting", name);
                    } else {
                        log.warn("{} worker-thread interrupted and  quitting", name);
                        Thread.currentThread().interrupt();
                    }
                }
            }
            suspendedThreadCount.decrementAndGet();
            return executable;
        }

        @Override
        public void run() {
            SequentialTask executable;
            Queue<SequentialTask> perKeyQueue;
            while (isRunning) {
                try {
                    perKeyQueue = null;
                    synchronized (taskQueue) {
                        executable = getNextTask();
                        if (null == executable) {
                            continue;
                        }
                        if (executable.getKey() != null) {
                            perKeyQueue = getKeyQueue(executable);
                        }
                    }
                    if (perKeyQueue != null) {
                        executeKeyQueue(perKeyQueue, executable);
                    } else if (executable.getKey() == null) {
                        taskByThreadMap.put(Thread.currentThread().getId(), executable);
                        executable.execute();
                    }
                } catch (Exception e) {
                    log.error("Exception ", e);
                } catch (Throwable e) {
                    log.error("Throwable ", e);
                }
            }
            log.warn("workerThread stopping");
        }

        /**
         * Get the keyQueue to execute for the given task
         * Precondition: synchronized(taskQueue)
         *
         * @param executable
         * @return null if delegate to another thread
         */
        private Queue<SequentialTask> getKeyQueue(SequentialTask executable) {
            String key = executable.getKey();
            Queue<SequentialTask> perKeyQueue = taskExecutionMap.get(key);
            if (perKeyQueue == null) {
                perKeyQueue = new LinkedBlockingDeque<>(maxKeyQueueSize);
                perKeyQueue.add(executable);
                taskExecutionMap.put(key, perKeyQueue);
                return perKeyQueue;
            }
            if (perKeyQueue.isEmpty()) {
                perKeyQueue.add(executable);
                //Another thread has just finished the main look below but has not removed the map
                //Should not happen when taskQueue is maintained until handOffKeyQueue is completed
                log.warn("{} execute: context switch onto key:{} on queue {}", name, key, perKeyQueue.hashCode());
                return perKeyQueue;
            }
            if (!perKeyQueue.offer(executable)) {
                log.warn("{} execute: ful-buffer: dropping oldest event on key:{} on queue:{} size:{}", name, key,
                        perKeyQueue.hashCode(), perKeyQueue.size());
                //Dropping oldest tasks to add new
                SequentialTask droppedTask = perKeyQueue.remove();
                if (droppedTask != null) {
                    droppedTask.completeExceptoinally(REJECTED_EXECUTION_EXCEPTION);
                }
                perKeyQueue.add(executable);
                return perKeyQueue;
            }
            return null;
        }

        /**
         * executes all task in the perKeyQueue and hand-off when complete
         *
         * @param perKeyQueue
         * @param firstExecutable
         */
        private void executeKeyQueue(Queue<SequentialTask> perKeyQueue, SequentialTask firstExecutable) {
            SequentialTask executable = firstExecutable;
            String key = executable.getKey();
            //MAin loop where work is done
            while (executable != null) {
                //leave the currently executing task as a singnal that a thread is still active on this key
                taskByThreadMap.put(Thread.currentThread().getId(), executable);
                executable.execute();
                synchronized (taskQueue) {
                    if (executable == perKeyQueue.peek()) {
                        perKeyQueue.remove();
                    } else {
                        //happens on full buffer when olders task was dropped to add new
                        log.warn("{} executeAsOwner: full-buffer: on perKeyQueue[{}].size[{}] for key:{} task:{}", name,
                                perKeyQueue.hashCode(), perKeyQueue.size(), key, executable);
                    }
                    executable = perKeyQueue.peek();
                    if (executable == null) {
                        handOffKeyQueue(perKeyQueue, key);
                    }
                }
            }
        }

        /**
         * removes the perKeyQueue from taskExecutionMap if still the owner
         * PreCondition: synchronized(taskQueue)  to avoid complex race conditions that are logged as warn
         *
         * @param perKeyQueue
         * @param key
         */
        private void handOffKeyQueue(Queue<SequentialTask> perKeyQueue, String key) {
            SequentialTask executable = perKeyQueue.peek();
            if (executable == null) {
                Queue<SequentialTask> currentQ = taskExecutionMap.get(key);
                if (currentQ == perKeyQueue) {
                    Queue<SequentialTask> removedQ = taskExecutionMap.remove(key);
                    if (!removedQ.isEmpty()) {
                        log.warn(
                                "{} handOffKeyQueue: perKeyQueue[{}].size[{}] removedQ[{}].size[{}] not empty for key:{}",
                                name, perKeyQueue.hashCode(), perKeyQueue.size(), removedQ.hashCode(), removedQ.size(),
                                key);
                    }
                    //signal the polling thread in waitUntilComplete
                    if (taskExecutionMap.isEmpty()) {
                        taskQueue.notifyAll();
                    }
                } else if (currentQ == null) {
                    //case:2 This thread has been suspended waiting for taskQueue lock and another thread a) took over its
                    //perKeyQueue in case:1  and b) completed processing ad c) removed perKeyQueue from the map
                    log.warn("{} handOffKeyQueue: perKeyQueue[{}].size[{}] not found for key:{}", name, perKeyQueue.hashCode(), perKeyQueue
                            .size(), key);
                } else {
                    //case:3 This thread has been suspended waiting for taskQueue lock and another thread completed case:@
                    //Then a third thread created a new perKeyQueue and has not yet removed the new perKeyQueue
                    log.warn("{} handOffKeyQueue: perKeyQueue[{}].size[{}] not same as currentQ[{}].size[{}] for key:{}", name, perKeyQueue
                            .hashCode(), perKeyQueue
                            .size(), currentQ.hashCode(), currentQ
                            .size(), key);
                }
            } else {
                //case:1 Another thread added a task to the queue before we got the taskQueue lock
                //It found an empty perKeyQueue in taskExecutionMap and took charge of executing it
                log.warn("{} handOffKeyQueue: context switch from key:{} on queue:{}", name, key, perKeyQueue.hashCode());
            }
        }
    }
    @Override
    public ExecutorBatch createBatch() {
        return new ExecutorBatchImpl(this);
    }


    @Override
    public boolean waitUntilComplete(long timeout, TimeUnit unit) {
        final long timeoutMsec = unit.toMillis(timeout) + 1;
        long remainingTimeNano = unit.toNanos(timeout);
        final long maxTimeNano = remainingTimeNano + System.nanoTime();
        int remainingTaskCount = taskQueue.size();
        int remainingQueuesCount = taskExecutionMap.size();
        int busyThreadCount;
        log.debug("{} waitUntilComplete tasks:{},queues:{}, idle-threads:{}, timeout in {} ms", name,
                remainingTaskCount, remainingQueuesCount, suspendedThreadCount, threadPoolSize, timeoutMsec);
        synchronized (taskQueue) {
            try {
                int loops = 0;
                while ((remainingTaskCount > 0 || remainingQueuesCount > 0) && remainingTimeNano > 0) {
                    taskQueue.wait(10);
                    remainingTaskCount = taskQueue.size();
                    remainingQueuesCount = taskExecutionMap.size();
                    remainingTimeNano = maxTimeNano - System.nanoTime();
                    if (++loops == 10) {
                        log.warn("{} waitUntilComplete; remaining tasks:{}, queues:{}, idle-threads:{}/{}, times:{}",
                                name, remainingTaskCount, remainingQueuesCount, suspendedThreadCount, threadPoolSize,
                                consecutiveFailedCompletes);
                        loops = 0;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            busyThreadCount = threadPoolSize - suspendedThreadCount.get();
        }
        if (remainingTaskCount != 0) {
            consecutiveFailedCompletes.incrementAndGet();
            log.warn(
                    "{} waitUntilComplete failed - remaining count-task:{}, count-queues:{},busy-threads:{}/{}, times:{}",
                    name, remainingTaskCount, remainingQueuesCount, busyThreadCount, threadPoolSize,
                    consecutiveFailedCompletes);
            return false;
        }
        if (remainingQueuesCount != 0) {
            consecutiveFailedCompletes.incrementAndGet();
            log.warn(
                    "{} waitUntilComplete failed - remaining count-queues:{}, count-task:{},busy-threads:{}/{}, times:{}",
                    name, remainingQueuesCount, remainingTaskCount, busyThreadCount, threadPoolSize,
                    consecutiveFailedCompletes);
            taskExecutionMap.forEach((key, queue) -> log
                    .warn("waitUntilComplete failed - key:{} tasks:{}", key, queue.size()));
            if (busyThreadCount == 0 && consecutiveFailedCompletes.get() > 3) {
                taskExecutionMap.forEach((key, queue) -> {
                    //Risk non-sequential execution to resolve 'orphaned' queue that has lost its thread
                    log.error("{} waitUntilComplete failed - resubmit key:{} tasks:{}", name, key,
                            queue.size());
                    seqExecutor.executeKeyQueue(queue, queue.peek());
                });
            } else if (busyThreadCount == 0 && consecutiveFailedCompletes.get() > 6) {
                log.error("waitUntilComplete failed - discarding {} queues", remainingQueuesCount);
                taskExecutionMap.clear();
            } else if (busyThreadCount > 0 && consecutiveFailedCompletes.get() > 2) {
                auditTasks();
            }
            return false;
        }
        consecutiveFailedCompletes.set(0);
        //wait-process-time does not include time to inject tasks
        log.debug("{} waitUntilComplete success, busy-threads:{}/{}, wait-batch-time-ms:{}", name, busyThreadCount, threadPoolSize,
                timeoutMsec - TimeUnit.NANOSECONDS.toMillis(remainingTimeNano));
        return true;
    }

    private void auditTasks() {
        //Stop the world synchronization required iterate map
        synchronized (taskQueue) {
            taskExecutionMap.values().forEach(queue -> {
                //Detect stuck threads(dead-locked threads)
                SequentialTask currentTask = queue.peek();
                if (currentTask != null) {
                    currentTask.audit();
                }
            });
        }
    }

    protected void auditThreads() {
        log.debug(" auditThreads {}", threadIds.size());
        //thread ids allow iteration of the map without stop-the-world synchronization
        threadIds.forEach(threadId -> {
            SequentialTask task = taskByThreadMap.get(threadId);
            if (task != null) {
                try {
                    task.audit();
                } catch (Exception e) {
                    log.error(" auditThreads exception", e);
                }
            } else {
                log.debug("auditThreads no task for threadID:{}", threadId);
            }
        });
    }

    @Override
    public void shutDown() {
        log.info("shutdown begin {}", this);
        isRunning = false;
        synchronized (taskQueue) {
            taskQueue.notifyAll();
        }
        scheduledExec.shutdown();
        backingExecutor.shutdown();
        waitUntilComplete(INJECTION_THROTTLE_TIME_MS, TimeUnit.MILLISECONDS);
        log.info("shutdown end {}", this);
    }

    @Override
    public boolean isShutDown() {
        return !isRunning;
    }


    @Override
    public String toString() {
        int queuedTaskCount = 0;
        synchronized (taskQueue) {
            queuedTaskCount = taskQueue.size();
        }
        return new StringBuffer().append("name:")
                .append(name)
                .append(", queued-tasks:")
                .append(queuedTaskCount)
                .append(", queue-count:")
                .append(taskExecutionMap.size())
                .append(", suspended-thread-count:")
                .append(suspendedThreadCount)
                .append(", thread-count:")
                .append(threadPoolSize).toString();
    }
}
