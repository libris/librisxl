package whelk.util

import java.util.concurrent.BlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RunnableFuture
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.regex.Matcher

import groovy.transform.CompileStatic

@CompileStatic
class ThreadPool {
    private BlockingQueue<Runnable> blockingQueue
    private final int THREAD_COUNT
    private int nextThreadNo = 0;
    private final ThreadPoolExecutor s_threadPool

    ThreadPool(int threadCount) {
        THREAD_COUNT = threadCount
        blockingQueue = new LinkedBlockingQueue<Runnable>()
//        s_threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadCount);
        s_threadPool = new ThreadPoolExecutor(threadCount, threadCount, 0, TimeUnit.MILLISECONDS, blockingQueue);
    }

    interface Worker<T> {
        void doWork(T t, int threadIndex)
    }

    /**
     * Will block until a thread becomes available to perform this work.
     * 
     * This version uses Java standard Executors, but implements the blocking wait
     * for execution to retain full interface compatibility.
     */
    def <T> void executeOnThread(T workLoad, Worker<T> worker) {
        Object MONITOR = new Object();
        boolean executionStarted = false;
        
        int threadNo;
        
        synchronized (this) {
            threadNo = nextThreadNo++;
        }

        Runnable runnable = new Runnable() {
            void run() {
                synchronized (MONITOR) {
                    executionStarted = true;
                    MONITOR.notifyAll();
                }
                String threadName = Thread.currentThread().getName();
//                    def Matcher threadNameMatcher = threadName =~ /pool-(\d+)-thread-(\d+)/
//                    int i = Integer.valueOf(threadNameMatcher.group(1));
                
                String noString;
                if (threadName.lastIndexOf('-') > 0) {
                    noString = threadName.substring(threadName.lastIndexOf('-'), threadName.length());
                } else {
                    noString = Integer.toString(threadNo);
                }
                
                worker.doWork(workLoad, Integer.parseInt(noString));
            }
        }
        

//        s_threadPool.queue.put(runnable);
//        blockingQueue.put(runnable);
        s_threadPool.execute(runnable)

        synchronized (MONITOR) {
            while (!executionStarted) {
                MONITOR.wait(100);
            }
        }
    }

    int getActiveThreadCount() {
        return s_threadPool.activeCount;
    }

    void joinAll() {
        s_threadPool.shutdown();
        
        for (int i=0; i < 20; i++) {
            if (s_threadPool.awaitTermination(5, TimeUnit.SECONDS)) return;
        }
    }
}
