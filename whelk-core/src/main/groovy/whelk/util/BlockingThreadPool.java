package whelk.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import whelk.exception.WhelkRuntimeException;
import whelk.meta.WhelkConstants;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * A thread pool with multiple virtual task queues.
 * If a task is submitted to a queue that is full, the caller is blocked.
 * <p>
 * <p>
 * The intended use is to pace a single producer that creates work faster than can be consumed by the pool.
 * For example reading a file and doing something expensive for each entry in the file.
 * <p>
 * <p>
 * Alas, there is no clean way of having a ThreadPoolExecutor with a bounded queue where the caller blocks
 * when the queue is full. (Best hacky option seems to be to subclass som Queue and override offer() with a call
 * to put(), since ThreadPoolExecutor calls offer() internally...)
 * Instead, use an unbounded shared queue and use a semaphore (per client) to put a bound on the number of tasks queued.
 */
public class BlockingThreadPool {
    private static final int DEFAULT_QUEUE_SIZE = 256;
    private final int queueSize;
    private final int purgeOutstandingEvery;

    private final ExecutorService pool;

    public BlockingThreadPool(String name, int poolSize, int queueSize) {
        this.queueSize = queueSize;
        this.purgeOutstandingEvery = queueSize * 6;
        
        pool = Executors.newFixedThreadPool(poolSize, getThreadFactory(name));
    }
    
    public BlockingThreadPool(String name, int poolSize) {
        this(name, poolSize, DEFAULT_QUEUE_SIZE);
    }

    /** A pool with a single task queue */ 
    public static SimplePool simplePool(int poolSize) {
        return new SimplePool(poolSize);
    }

    // The returned queue is not thread safe
    public Queue getQueue() {
        return new Queue();
    }
    
    public void shutdown() {
        pool.shutdown();
    }

    public class Queue {
        private final Semaphore queuePermits = new Semaphore(queueSize);
        private List<Future<?>> outstandingTasks = new ArrayList<>();
        private int totalNumQueued = 0;

        private Queue() {
        }
        
        public void submit(Runnable task) throws WhelkRuntimeException {
            queuePermits.acquireUninterruptibly();

            Runnable r = () -> {
                try {
                    task.run();
                } finally {
                    queuePermits.release();
                }
            };

            outstandingTasks.add(pool.submit(r));

            // try to limit outstandingTasks to an arbitrary smallish size
            if (totalNumQueued++ % purgeOutstandingEvery == 0) {
                var done = outstandingTasks.stream()
                        .collect(Collectors.partitioningBy(Future::isDone, Collectors.toCollection(ArrayList::new)));

                checkResult(done.get(true));
                outstandingTasks = done.get(false);
            }
        }

        public void awaitAll() throws WhelkRuntimeException {
            checkResult(outstandingTasks);
        }

        public void cancelAll() {
            queuePermits.release(outstandingTasks.size());
            outstandingTasks.forEach(t -> t.cancel(false));
        }

        private void checkResult(List<Future<?>> tasks) {
            for (Future<?> f : tasks) {
                try {
                    f.get();
                } catch (ExecutionException e) {
                    throw new WhelkRuntimeException("Task threw exception: " + e.getMessage(), e.getCause());
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    /**
     * A pool with a single task queue that blocks the caller if the queue is full
     */
    public static class SimplePool {
        BlockingThreadPool pool;
        Queue queue;
        
        public SimplePool(int poolSize) {
            this.pool = new BlockingThreadPool(SimplePool.class.getSimpleName(), poolSize, poolSize * 10);
            this.queue = pool.getQueue();
        }
        
        public void submit(Runnable task) {
            queue.submit(task);
        }
        
        // Mimic the old ThreadPool interface
        // (saves the caller from having to create a temporary effectively final variable to pass to the lambda)
        public <T> void submit(T workLoad, Consumer<T> workerFunction) {
            this.submit(() -> workerFunction.accept(workLoad));
        }

        public void awaitAll() {
            queue.awaitAll();
        }
        
        public void awaitAllAndShutdown() {
            queue.awaitAll();
            pool.shutdown();
        }
    }

    private static ThreadFactory getThreadFactory(String name) {
        return new ThreadFactoryBuilder()
                .setNameFormat(name + "-%d")
                .setThreadFactory(new ThreadFactory() {
                    ThreadGroup group = new ThreadGroup(WhelkConstants.BATCH_THREAD_GROUP);

                    public Thread newThread(Runnable runnable) {
                        return new Thread(group, runnable);
                    }
                })
                .build();
    }
}
