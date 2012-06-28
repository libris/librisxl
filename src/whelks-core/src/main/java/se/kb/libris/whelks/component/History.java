package se.kb.libris.whelks.component;

import java.util.Date;

import se.kb.libris.whelks.LogEntry;

public interface History {
    public Iterable<LogEntry> updates(Date since);
}
