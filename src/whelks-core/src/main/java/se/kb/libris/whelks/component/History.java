package se.kb.libris.whelks.component;

import java.util.Date;
import java.util.Collection;

import se.kb.libris.whelks.LogEntry;

public interface History {
    public static final int BATCH_SIZE = 1000;
    public HistoryUpdates updates(Date since);
    public HistoryUpdates updates();

    public class HistoryUpdates {
        private Collection<LogEntry> updates;
        private String nextToken;

        public HistoryUpdates(Collection<LogEntry> u, String n) {
            this.updates = u;
            this.nextToken = n;
        }

        public String getNextToken() { return this.nextToken; }
        public Collection<LogEntry> getUpdates() { return this.updates; }
    }
}
