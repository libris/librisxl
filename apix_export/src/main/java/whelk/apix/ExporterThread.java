package whelk.apix;

import java.time.ZonedDateTime;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExporterThread extends Thread
{
    /**
     * This atomic boolean may be toggled from outside, causing the thread to stop exporting and return
     */
    public AtomicBoolean stopAtOpportunity = new AtomicBoolean(false);

    /**
     * The "from" parameter. The exporter will export everything with a (modified) timestamp >= this value.
     */
    private final ZonedDateTime exportNewerThan;

    /**
     * The "until" parameter. The exporter will export everything with a (modified) timestamp < this value.
     */
    private final ZonedDateTime exportOlderThan;

    /**
     * General APIX endpoint parameters (URL etc)
     */
    private final Properties apixProperties;

    /**
     * UI callback interface
     */
    UI ui;

    public ExporterThread(Properties apixProperties, ZonedDateTime exportNewerThan,
                          ZonedDateTime exportOlderThan, UI ui)
    {
        this.apixProperties = apixProperties;
        this.exportNewerThan = exportNewerThan;
        this.exportOlderThan = exportOlderThan;
        this.ui = ui;
    }

    public void run()
    {
        //ui.outputText("Exit!" + apixProperties.getProperty("apixUrl"));

        int i = 0;
        while( !stopAtOpportunity.get() )
        {
            //System.out.println("wtf, " + i);
            if ( i < 3 )
            {
                ++i;
                ui.outputText("Pseudo call: " + apixProperties.getProperty("apixUrl") + " " + i);
            }
        }

        ui.outputText("Exit!");
    }
}
