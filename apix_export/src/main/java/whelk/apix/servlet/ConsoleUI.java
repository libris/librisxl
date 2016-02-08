package whelk.apix.servlet;

import java.time.ZonedDateTime;
import java.util.Properties;
import whelk.util.PropertyLoader;

/**
 * Program entry when running in CLI-mode.
 */
public class ConsoleUI implements UI
{
    ZonedDateTime parseFrom(String[] args)
    {
        return null;
    }

    ZonedDateTime parseUntil(String[] args)
    {
        return null;
    }

    public ConsoleUI(String[] args)
    {
        Properties apixProperties = PropertyLoader.loadProperties("apix");

        final ExporterThread exporterThread = new ExporterThread(
                apixProperties,
                parseFrom(args),
                parseUntil(args),
                this);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                exporterThread.stopAtOpportunity.set(true);
                try {
                    exporterThread.join();
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        });

        exporterThread.start();
    }

    public void outputText(String text)
    {
        System.out.println(text);
    }

    public static void main(String[] args)
    {
        new ConsoleUI(args);
    }
}
