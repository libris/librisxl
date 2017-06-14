package whelk.importer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Opens a file and locks it. The point of this is that the lock can't/won't
 * be obtained until whoever is writing the file has finished, guaranteeing a complete file.
 */
class ExclusiveFile implements AutoCloseable
{
    private FileChannel channel;
    private FileLock lock;

    ExclusiveFile(Path path)
            throws IOException
    {
        channel = FileChannel.open(path, StandardOpenOption.READ);
        lock = channel.lock(0L, Long.MAX_VALUE, true);
    }

    public void close()
            throws IOException
    {
        try { lock.release(); } catch (Exception e) { /* ignore */ }
        try { channel.close(); } catch (Exception e) { /* ignore */ }
    }

    InputStream getInputStream()
    {
        return Channels.newInputStream(channel);
    }
}
