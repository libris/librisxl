package whelk.exception;

import java.util.List;
import java.util.ArrayList;

public class WhelkAddException extends WhelkException {

    List<String> failedIdentifiers = new ArrayList<String>();

    public WhelkAddException(String ident) {
        super(ident);
        this.failedIdentifiers.add(ident);
    }

    public WhelkAddException(List<String> idents) {
        super("Failed adding " + idents.size() + " documents.");
        this.failedIdentifiers.addAll(idents);
    }

    public WhelkAddException(Throwable t, List<String> idents) {
        super(t);
        this.failedIdentifiers.addAll(idents);
    }

    public WhelkAddException(String msg, List<String> idents) {
        super(msg);
        this.failedIdentifiers.addAll(idents);
    }

    public WhelkAddException(String msg, Throwable t, List<String> idents) {
        super(msg, t);
        this.failedIdentifiers.addAll(idents);
    }

    public List<String> getFailedIdentifiers() {
        return this.failedIdentifiers;
    }
}

