package whelk.apix;

import java.time.ZoneId;
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
        for (int i = 0; i < args.length; ++i)
        {
            if ("-from".equals(args[i]) && i+1 < args.length)
            {
                return ZonedDateTime.parse(args[i+1], DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.of("UTC")));
            }
        }

        return null;
    }

    public ConsoleUI(String[] args)
    {
        Properties apixProperties = PropertyLoader.loadProperties("secret");

        final ExporterThread exporterThread = new ExporterThread(
                apixProperties,
                parseFrom(args),
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

    public void setCurrentTimeStamp(ZonedDateTime zdt) {}

    public void outputText(String text)
    {
        System.out.println(text);
    }

    public static void main(String[] args)
    {
        new ConsoleUI(args);
    }
}
