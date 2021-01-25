package whelk.exception;

public class StaleUpdateException extends Exception {
    public StaleUpdateException(String msg) {
        super (msg);
    }
}
