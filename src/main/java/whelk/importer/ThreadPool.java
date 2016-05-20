package whelk.importer;

public class ThreadPool
{
    private final int THREAD_COUNT;
    private final Thread[] s_threadPool;

    public ThreadPool(int threadCount)
    {
        THREAD_COUNT = threadCount;
        s_threadPool = new Thread[THREAD_COUNT];
    }

    public interface Worker<T>
    {
        void doWork(T t);
    }

    /**
     * Will block until a thread becomes available to perform this work.
     */
    public <T> void executeOnThread(T workLoad, Worker<T> worker)
    {
        // Find a suitable thread from the pool to do the work

        int i = 0;
        while(true)
        {
            i++;
            if (i == THREAD_COUNT)
            {
                i = 0;
                Thread.yield();
            }

            if (s_threadPool[i] == null || s_threadPool[i].getState() == Thread.State.TERMINATED)
            {
                s_threadPool[i] = new Thread(new Runnable()
                {
                    public void run()
                    {
                        worker.doWork(workLoad);
                    }
                });
                s_threadPool[i].start();
                return;
            }
        }
    }

    public int getActiveThreadCount()
    {
        int activeThreadCount = 0;
        for (int i = 0; i < THREAD_COUNT; ++i)
        {
            if (s_threadPool[i] != null && s_threadPool[i].getState() != Thread.State.TERMINATED)
                ++activeThreadCount;
        }
        return activeThreadCount;
    }

    public void joinAll()
            throws InterruptedException
    {
        for (int i = 0; i < THREAD_COUNT; ++i)
        {
            if (s_threadPool[i] != null)
                s_threadPool[i].join();
        }
    }

}
