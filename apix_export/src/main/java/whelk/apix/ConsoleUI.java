package whelk.apix;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import whelk.util.PropertyLoader;

/**
 * Program entry when running in CLI-mode.
 */
public class ConsoleUI implements UI
{
    ZonedDateTime parseFrom(String[] args)
    {
        for (int i = 1; i < args.length; ++i)
        {
            if ("-from".equals(args[i]) && i+1 < args.length)
            {
                return ZonedDateTime.parse(args[i], DateTimeFormatter.ISO_INSTANT);
            }
        }

        return null;
    }

    ZonedDateTime parseUntil(String[] args)
    {
        for (int i = 1; i < args.length; ++i)
        {
            if ("-until".equals(args[i]) && i+1 < args.length)
            {
                return ZonedDateTime.parse(args[i], DateTimeFormatter.ISO_INSTANT);
            }
        }

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
