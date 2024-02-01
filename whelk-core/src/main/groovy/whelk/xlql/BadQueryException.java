package whelk.xlql;

import whelk.exception.InvalidQueryException;

public class BadQueryException extends InvalidQueryException {
    public BadQueryException(String s) {
        super(s);
    }
}
