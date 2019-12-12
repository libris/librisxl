package whelk.exception;

public class ElasticIOException extends WhelkRuntimeException {
    public ElasticIOException(String msg, Throwable t) {
        super(msg, t);
    }
}
