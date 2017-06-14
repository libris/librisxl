package whelk.exception;

/**
 * Created by markus on 2016-04-04.
 */
public class ModelValidationException extends RuntimeException {
    ModelValidationException(String message) {
        super(message);
    }
}
