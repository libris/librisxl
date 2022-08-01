package whelk.importer;

public class ThreadPool
{
    public interface Worker<T>
    {
        void doWork(T t);
    }
}
