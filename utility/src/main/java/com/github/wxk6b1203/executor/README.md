# Executor
This module provides utility classes for managing and executing tasks using thread pools. It includes features for creating fixed and cached thread pools, scheduling tasks, and handling task execution with customizable options.

## ScaleFirstExecutor
- Parameters: `corePoolSize`, `maxPoolSize`, `queueCapacity`, optional `ThreadFactory`, and `RejectedExecutionHandler`.
- Behavior: tries to spawn new workers up to `maxPoolSize` before placing tasks in the queue; once at max, tasks fill the queue up to `queueCapacity`, then are rejected.
- Default keep-alive: 60s for non-core threads.
- Rejection: defaults to `ThreadPoolExecutor.AbortPolicy`.

```java
ExecutorService executor = new ScaleFirstExecutor(
        /* corePoolSize */ 2,
        /* maxPoolSize  */ 8,
        /* queueCapacity*/ 16);
```
