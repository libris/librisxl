package se.kb.libris.export.listener;

import oaij.client.Identifier;
import oaij.client.ListIdentifiers;
import oaij.client.OaiPmhClient;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.BlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

class ListenerThread extends Thread {
    OaiPmhClient client = null;
    Properties configProperties, exportProperties;
    String timestampFile, latestDatestamp = null, base;
    BlockingDeque<String> bdq;
    boolean run;
        
    ListenerThread(String base, Properties configProperties, Properties exportProperties, String timestampFile, BlockingDeque<String> bdq) {
        this.configProperties = configProperties;
        this.exportProperties = exportProperties;
        this.timestampFile = timestampFile;
        this.bdq = bdq;
        this.base = base;
        run = true;

        client = new OaiPmhClient(base);
        if (!configProperties.getProperty("User", "").equals("")) {
            client.withCredentials(
                    configProperties.getProperty("User"),
                    configProperties.getProperty("Password"));
        }
    }
    
    public String getLatestDatestamp() throws IOException {
        if (latestDatestamp != null) return latestDatestamp;

        if (timestampFile != null && new File(timestampFile).exists()) {
            File f = new File(timestampFile);
            
            try (Scanner s = new Scanner(f)) {
                s.useDelimiter("\\Z");
                latestDatestamp = s.next();
                Logger.getGlobal().log(Level.INFO, "Read latest datestamp from " + timestampFile + " (" + latestDatestamp + ")");
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else {
            latestDatestamp = client.identify().getResponseDate().asString();
        }
        
        return latestDatestamp;
    }
    
    public void setLatestDatestamp(String latestDatestamp) throws IOException {
        if (timestampFile != null) {
            FileWriter fw = new FileWriter(timestampFile);
            fw.write(latestDatestamp);
            fw.close();
        }
        
        this.latestDatestamp = latestDatestamp;
        
        Logger.getGlobal().log(Level.CONFIG, "latestDatestamp for " + base + " is now " + latestDatestamp);
    }
    
    @Override
    public void run() {
        Logger.getGlobal().log(Level.INFO, "ListenerThread enter (" + base + ")");
            
        ListIdentifiers li = null;
        int sleep = 1000;
        while (run) {
            try {
                if (li != null && li.hasNext()) {
                    Identifier id = li.next();

                    // mask out identifiers with datestamp >= responseDate
                    if (li.getResponseDate().compareTo(id.getDatestamp()) > 0) {
                        Logger.getGlobal().log(Level.INFO, "Adding URI " + id.getIdentifier());
                        bdq.offer(id.getIdentifier());
                    }
                } else {
                    try {
                        if (li != null) {
                            Thread.sleep(5000);
                            li.close();
                        }
                        
                        li = client.listIdentifiers("marcxml").withFrom(getLatestDatestamp());
                        setLatestDatestamp(li.getResponseDate().asString());
                    } catch (InterruptedException e) {
                        run = false;
                        throw new RuntimeException(e);
                    }
                }
                
                sleep = 1000;
            } catch (Exception e) {
                if (run) {
                    Logger.getGlobal().log(Level.WARNING, "Exception in main loop, retry in " + sleep + " msecs. (" + base + ")", e);
                    try {
                        try { li.close(); } catch (Exception e2) {}
                        try { li = null; } catch (Exception e2) {}
                        if (sleep < 5*60*1000) sleep *= 2;
                        Thread.sleep(sleep);
                    } catch (InterruptedException ex) {
                    }
                }
            }
        }
        
        Logger.getGlobal().log(Level.INFO, "ListenerThread exit (" + base + ")");
        
        client.close();
    }
}
