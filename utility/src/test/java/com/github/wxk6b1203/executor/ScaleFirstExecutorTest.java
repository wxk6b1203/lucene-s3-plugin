package com.github.wxk6b1203.executor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ScaleFirstExecutorTest {

    private ScaleFirstExecutor executor;

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    void growsThreadsBeforeQueueing() throws Exception {
        executor = new ScaleFirstExecutor(1, 3, 2);
        CountDownLatch blockLatch = new CountDownLatch(1);

        Runnable blockingTask = () -> {
            try {
                // Wait but surface unexpected interruption
                if (!blockLatch.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("latch wait timed out");
                }
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        };

        executor.execute(blockingTask);
        executor.execute(blockingTask);
        executor.execute(blockingTask);

        awaitPoolSize(3);
        assertEquals(0, executor.getQueue().size(), "queue should stay empty while pool can grow");

        executor.execute(() -> {});
        awaitQueueSize(1);

        executor.execute(() -> {});
        awaitQueueSize(2);

        assertThrows(RejectedExecutionException.class, () -> executor.execute(() -> {}));
        blockLatch.countDown();
    }

    private void awaitPoolSize(int expected) throws InterruptedException {
        int attempts = 50;
        for (int i = 0; i < attempts; i++) {
            if (executor.getPoolSize() >= expected) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(20);
        }
        assertEquals(expected, executor.getPoolSize(), "pool size did not reach expected value in time");
    }

    private void awaitQueueSize(int expected) throws InterruptedException {
        for (int i = 0; i < 50; i++) {
            if (executor.getQueue().size() >= expected) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(20);
        }
        assertEquals(expected, executor.getQueue().size(), "queue size did not reach expected value in time");
    }

    @Test
    public void testVirtualThread() {
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            executor.execute(() -> {
                System.out.println("Hello from a virtual thread!");
            });
        }

    }
}
