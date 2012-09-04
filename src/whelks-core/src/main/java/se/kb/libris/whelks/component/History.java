package se.kb.libris.whelks.component;

import java.util.Date;
import java.util.Collection;

import se.kb.libris.whelks.LogEntry;

public interface History {
    public static final int BATCH_SIZE = 1000;
    public Iterable<LogEntry> updates(Date since);
}
