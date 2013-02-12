package se.kb.libris.whelks.component;

import java.util.Date;
import java.util.Collection;

import se.kb.libris.whelks.Document;

public interface History extends Component {
    public static final int BATCH_SIZE = 1000;
    public Iterable<Document> updates(Date since);
}
