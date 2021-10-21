package whelk.util

import groovy.transform.CompileStatic

@CompileStatic
class ThreadPool {
    private final int THREAD_COUNT
    private final Thread[] s_threadPool

    ThreadPool(int threadCount) {
        THREAD_COUNT = threadCount
        s_threadPool = new Thread[THREAD_COUNT]
    }

    interface Worker<T> {
        void doWork(T t, int threadIndex)
    }

    /**
     * Will block until a thread becomes available to perform this work.
     */
    def <T> void executeOnThread(T workLoad, Worker<T> worker) {
        // Find a suitable thread from the pool to do the work

        int i = 0
        while(true) {
            i++
            if (i == THREAD_COUNT) {
                i = 0
                Thread.yield()
            }

            if (s_threadPool[i] == null || s_threadPool[i].getState() == Thread.State.TERMINATED) {
                s_threadPool[i] = new Thread(new Runnable() {
                    void run() {
                        worker.doWork(workLoad, i)
                    }
                })
                s_threadPool[i].start()
                return
            }
        }
    }

    int getActiveThreadCount() {
        int activeThreadCount = 0
        for (int i = 0; i < THREAD_COUNT; ++i) {
            if (s_threadPool[i] != null && s_threadPool[i].getState() != Thread.State.TERMINATED)
                ++activeThreadCount
        }
        return activeThreadCount
    }

    void joinAll()
            throws InterruptedException {
        for (int i = 0; i < THREAD_COUNT; ++i) {
            if (s_threadPool[i] != null)
                s_threadPool[i].join()
        }
    }

}
