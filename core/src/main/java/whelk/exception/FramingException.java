package whelk.exception;

/**
 * Created by markus on 2015-10-20.
 */
public class FramingException extends RuntimeException {

    public FramingException(String message) {
        super(message);
    }

    public FramingException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
