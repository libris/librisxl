package whelk.export.marc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import whelk.exception.WhelkRuntimeException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

/**
 * A thread pool with multiple virtual task queues.
 * If a task is submitted to a queue that is full, the caller is blocked.
 * <p>
 * <p>
 * Alas, there is no clean way of having a ThreadPoolExecutor with a bounded queue where the caller blocks
 * when the queue is full. (Best hacky option seems to be to subclass som Queue and override offer() with a call
 * to put(), since ThreadPoolExecutor calls offer() internally...)
 * Instead, use an unbounded shared queue and use a semaphore (per client) to put a bound on the number of tasks queued.
 */
class BlockingThreadPool {
    private static final int QUEUE_SIZE = 256;
    private static final int PURGE_OUTSTANDING_EVERY = QUEUE_SIZE * 6;

    private final ExecutorService pool;

    public BlockingThreadPool(String name, int poolSize) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat(name + "-%d")
                .build();

        pool = Executors.newFixedThreadPool(poolSize, threadFactory);
    }

    // The returned queue is not thread safe
    Queue getBlockingQueue() {
        return new BlockingQueue();
    }

    // The returned queue is not thread safe
    Queue getNonBlockingQueue() {
        return new NonBlockingQueue();
    }

    public class BlockingQueue extends Queue {
        private final Semaphore queuePermits = new Semaphore(QUEUE_SIZE);
        public void submit(Runnable task) throws WhelkRuntimeException {
            queuePermits.acquireUninterruptibly();

            Runnable r = () -> {
                try {
                    task.run();
                } finally {
                    queuePermits.release();
                }
            };

            submitInternal(r);
        }

        public void cancelAll() {
            queuePermits.release(outstandingTasks.size());
            outstandingTasks.forEach(t -> t.cancel(false));
        }
    }

    public class NonBlockingQueue extends Queue {

        @Override
        public void submit(Runnable task) throws WhelkRuntimeException {
            submitInternal(task);
        }

        public void cancelAll() {
            outstandingTasks.forEach(t -> t.cancel(false));
        }
    }
    
    public abstract class Queue {
        protected ConcurrentLinkedQueue<Future<?>> outstandingTasks = new ConcurrentLinkedQueue<>();
        private int totalNumQueued = 0;

        private Queue() {
        }

        public abstract void submit(Runnable task) throws WhelkRuntimeException;
        
        protected void submitInternal(Runnable r) {
            outstandingTasks.add(pool.submit(r));

            // try to limit outstandingTasks to an arbitrary smallish size
            if (totalNumQueued++ % PURGE_OUTSTANDING_EVERY == 0) {
                for (int i = 0 ; i < outstandingTasks.size() ; i++) {
                    Future<?> f = outstandingTasks.poll();
                    if (f == null) {
                        break;
                    }
                    
                    if (f.isDone()) {
                        checkResult(f);
                    }
                    else {
                        outstandingTasks.add(f);
                    }
                }
            }
        }

        public void awaitAll() throws WhelkRuntimeException {
            Future<?> task;
            while ((task = outstandingTasks.poll()) != null) {
                checkResult(task);
            }
        }

        public abstract void cancelAll();

        private void checkResult(List<Future<?>> tasks) {
            for (Future<?> task : tasks) {
                checkResult(task);
            }
        }

        private void checkResult(Future<?> task) {
            try {
                task.get();
            } catch (ExecutionException e) {
                throw new WhelkRuntimeException("Task threw exception: " + e.getMessage(), e.getCause());
            } catch (InterruptedException ignored) {}
        }
    }
}
