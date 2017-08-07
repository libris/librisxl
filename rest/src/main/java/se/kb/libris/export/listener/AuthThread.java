package se.kb.libris.export.listener;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import oaij.client.*;

class AuthThread extends Thread {
    OaiPmhClient client = null;
    Properties configProperties, exportProperties;
    String timestampFile, latestDatestamp = null;
    BlockingDeque<String> bdq;
    boolean run;
        
    AuthThread(Properties configProperties, Properties exportProperties, String timestampFile, BlockingDeque<String> bdq) {
        this.configProperties = configProperties;
        this.exportProperties = exportProperties;
        this.timestampFile = timestampFile;
        this.bdq = bdq;
        run = true;

        client = new OaiPmhClient(configProperties.getProperty("BibBaseUrl"));
        if (!configProperties.getProperty("User", "").equals("")) {
            client.withCredentials(
                    configProperties.getProperty("User"),
                    configProperties.getProperty("Password"));
        }
    }
    
    public String getLatestDatestamp() throws IOException {
        if (latestDatestamp != null) return latestDatestamp;

        if (timestampFile != null) {
            File f = new File(timestampFile);
            
            if (f.exists()) {
                try (Scanner s = new Scanner(f)) {
                    s.useDelimiter("\\Z");
                    latestDatestamp = s.next();
                    Logger.getGlobal().log(Level.INFO, "Read latest datestamp from file (" + latestDatestamp + ")");
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        } else {
            setLatestDatestamp(client.identify().getResponseDate().asString());
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
        
        //Logger.getGlobal().log(Level.CONFIG, "latestDatestamp is now " + latestDatestamp);
    }
    
    @Override
    public void run() {
        Logger.getGlobal().log(Level.INFO, "AuthThread enter");
            
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
                        Thread.sleep(5000);
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
                    Logger.getGlobal().log(Level.WARNING, "Exception in main loop, retry in " + sleep + " msecs.", e);
                    try {
                        if (sleep < 5*60*1000) sleep *= 2;
                        Thread.sleep(sleep);
                    } catch (InterruptedException ex) {
                    }
                }
            }
        }
        
        Logger.getGlobal().log(Level.INFO, "AuthThread exit");
        
        client.close();
    }
}
