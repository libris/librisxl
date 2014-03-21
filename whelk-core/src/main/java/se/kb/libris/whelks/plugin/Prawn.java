package se.kb.libris.whelks.plugin;

import java.util.concurrent.BlockingQueue;

public interface Prawn extends Runnable {
    BlockingQueue getQueue();
    void deactivate();
    public String getTrigger();
}

