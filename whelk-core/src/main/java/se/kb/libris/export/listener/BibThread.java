package se.kb.libris.export.listener;

import java.util.logging.Logger;
import java.util.Properties;
import java.util.concurrent.BlockingDeque;
import java.util.logging.Level;
import oaij.client.Identifier;
import oaij.client.OaiPmhClient;
import oaij.client.Record;

class BibThread extends Thread {
    Properties configProperties, exportProperties;
    String timestampFile;
    BlockingDeque<String> bdq;
    boolean run;
    
    BibThread(Properties configProperties, Properties exportProperties, String timestampFile, BlockingDeque<String> bdq) {
        this.configProperties = configProperties;
        this.exportProperties = exportProperties;
        this.timestampFile = timestampFile;
        this.bdq = bdq;
        run = true;
    }

    @Override
    public void run() {
        try (OaiPmhClient client = new OaiPmhClient(configProperties.getProperty("BibBaseUrl"))) {
            String latestDatestamp, lastUri = "";

            Logger.getGlobal().log(Level.INFO, "BibThread enter");

            if (!configProperties.getProperty("User", "").equals("")) {
                client.withCredentials(
                        configProperties.getProperty("User"),
                        configProperties.getProperty("Password"));
            }                

            // get start date
            if (timestampFile != null) {
                latestDatestamp = null;
            } else {
                latestDatestamp = client.identify().getResponseDate().asString();
            }
            
            while (run) {
                String lastLatestDatestamp = latestDatestamp;
                for (Identifier i: client.listIdentifiers("marcxml").withFrom(latestDatestamp)) {
                    if (!latestDatestamp.equals(i.getDatestamp().asString()) || !lastUri.equals(i.getIdentifier())) {
                        Logger.getGlobal().log(Level.INFO, "Record " + i.getIdentifier() + " changed at " + i.getDatestamp().asString());                    
                        lastUri = i.getIdentifier();
                        bdq.offer(i.getIdentifier());
                    }
                    
                    if (i.getDatestamp().asString().compareTo(latestDatestamp) > 0) latestDatestamp = i.getDatestamp().asString();
                }
                
                Thread.sleep(5000);
            }
        } catch (Exception e) {
            
        } finally {
            Logger.getGlobal().log(Level.INFO, "BibThread exiting");
        }
    }
}
