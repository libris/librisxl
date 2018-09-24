package se.kb.libris.export.listener;

import java.util.Properties;
import java.util.concurrent.BlockingDeque;

class HoldThread extends Thread {

    HoldThread(Properties configProperties, Properties exportProperties, String timestampFile, BlockingDeque<String> bdq) {
    }

    @Override
    public void start() {
    }
}
