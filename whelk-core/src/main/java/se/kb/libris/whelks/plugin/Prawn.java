package se.kb.libris.whelks.plugin;

import java.util.concurrent.BlockingQueue;

public interface Prawn extends Runnable {
    public BlockingQueue getQueue();
    public void deactivate();
    public String getTrigger();
}

