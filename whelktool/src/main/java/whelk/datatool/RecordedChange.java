package whelk.datatool;

import whelk.Document;

public record RecordedChange(Document before, Document after, int number) implements Comparable<RecordedChange> {

    @Override
    public int compareTo(RecordedChange o) {
        return this.number - o.number;
    }
}
