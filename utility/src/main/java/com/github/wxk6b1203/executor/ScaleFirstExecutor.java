package com.github.wxk6b1203.executor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A thread pool executor that grows the worker count up to {@code maximumPoolSize}
 * before enqueuing tasks. Once the pool is at its maximum, tasks are queued up to
 * the configured {@code queueCapacity}; submissions beyond that are rejected using
 * the provided {@link RejectedExecutionHandler} (defaults to {@link AbortPolicy}).
 */
public class ScaleFirstExecutor extends ThreadPoolExecutor {

    private static final long DEFAULT_KEEP_ALIVE_SECONDS = 60L;

    public ScaleFirstExecutor(int corePoolSize, int maximumPoolSize, int queueCapacity) {
        this(corePoolSize, maximumPoolSize, queueCapacity,
                Executors.defaultThreadFactory(), new AbortPolicy());
    }

    public ScaleFirstExecutor(int corePoolSize, int maximumPoolSize, int queueCapacity,
                              ThreadFactory threadFactory,
                              RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, DEFAULT_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS,
                createQueue(queueCapacity), threadFactory, handler);
        ScalingQueue queue = (ScalingQueue) getQueue();
        queue.setExecutor(this);
    }

    private static BlockingQueue<Runnable> createQueue(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("queueCapacity must be greater than zero");
        }
        return new ScalingQueue(capacity);
    }

    /**
     * Queue that refuses to enqueue while the pool can still grow, forcing the
     * executor to create new workers up to {@code maximumPoolSize} before falling
     * back to queuing.
     */
    private static final class ScalingQueue extends LinkedBlockingQueue<Runnable> {
        private static final long serialVersionUID = 1L;

        private volatile ThreadPoolExecutor executor;

        ScalingQueue(int capacity) {
            super(capacity);
        }

        void setExecutor(ThreadPoolExecutor executor) {
            this.executor = executor;
        }

        @Override
        public boolean offer(Runnable runnable) {
            ThreadPoolExecutor exec = executor;
            if (exec != null && exec.getPoolSize() < exec.getMaximumPoolSize()) {
                // Force the executor to try to add a new worker instead of queueing.
                return false;
            }
            return super.offer(runnable);
        }
    }
}

