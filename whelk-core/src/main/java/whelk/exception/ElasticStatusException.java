package whelk.exception;

public class ElasticStatusException extends WhelkRuntimeException {
    private int statusCode;

    public ElasticStatusException(String msg, int statusCode) {
        super(msg);
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }

    @Override
    public String getMessage() {
        return statusCode + ": " + super.getMessage();
    }
}
