package se.kb.libris.export.listener;


import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

public class OaiPmhListener {
    public static void printOptions() {
        System.err.println("usage: java ... se.kb.libris.export.listener.OaiPmhListener [options] <export profile> <settings>");
        System.err.println("options:");
        System.err.println("  --directory <dir>      : create records under <dir>");
        System.err.println("  --postURL <URL>        : POST updated records to <URL>");
        System.err.println("  --timestampFile <file> : use <file> to store latest timestamp");
        System.err.println("  --listen               : print URIs for updated records to stdout (default)");
        System.err.println("  --silent               : no output");
        System.err.println("  --verbose              : more output");
        System.err.println("  --debug                : include diagnostic output");
    }

    public static void main(String args[]) throws MalformedURLException, IOException, InterruptedException {
        for (String s: args) System.out.println(s);
        System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tF %1$tT %4$s %5$s%6$s%n");
        Logger.getGlobal().setLevel(Level.ALL);

        if (args.length < 2) {
            printOptions();
            System.exit(1);
        }

        String directory = null, timestampFile = "timestamp";
        URL postURL = null;
        boolean debug = false, silent = false;

        int n=0;
        while (n < args.length-2) {
            String arg = args[n++];

            if (arg.equals("--directory")) {
                directory = args[n++];
            } else if (arg.equals("--postURL")) {
                postURL = new URL(args[n++]);
            } else if (arg.equals("--timestampFile")) {
                timestampFile = args[n++];
            } else if (arg.equals("--help") || arg.equals("-h")) {
                printOptions();
                System.exit(0);
            } else {
                System.err.println("unknown argument '" + arg + "'");
                printOptions();
                System.exit(1);
            }
        }

        Properties exportProperties = new Properties();
        Properties configProperties = new Properties();
        
        exportProperties.load(new FileInputStream(args[args.length-2]));
        configProperties.load(new FileInputStream(args[args.length-1]));
        BlockingDeque<String> bdq = new LinkedBlockingDeque<String>(1000);
        ListenerThread authThread = new ListenerThread(configProperties.getProperty("AuthBaseUrl"), configProperties, exportProperties, (timestampFile == null)? null:(timestampFile + ".auth"), bdq);
        ListenerThread bibThread = new ListenerThread(configProperties.getProperty("BibBaseUrl"), configProperties, exportProperties, (timestampFile == null)? null:(timestampFile + ".bib"), bdq);
        ListenerThread holdThread = new ListenerThread(configProperties.getProperty("HoldBaseUrl"), configProperties, exportProperties, (timestampFile == null)? null:(timestampFile + ".mfhd"), bdq);

        // start threads
        if (!exportProperties.getProperty("authtype", "none").equalsIgnoreCase("none")) {
            authThread.start();
        }

        if (!exportProperties.getProperty("holdtype", "none").equalsIgnoreCase("none")) {
            holdThread.start();
        }

        bibThread.start();

        // main loop
        while (true) {
            String bibId = bdq.take();
            System.out.println(bibId);
        }
    }
}
