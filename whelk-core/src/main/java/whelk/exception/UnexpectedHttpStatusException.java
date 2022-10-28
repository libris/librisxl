package whelk.exception;

public class UnexpectedHttpStatusException extends WhelkRuntimeException {
    private final int statusCode;

    public UnexpectedHttpStatusException(String msg, int statusCode) {
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

    public static boolean isBadRequest(Exception e) {
        return isStatus(e, 400);
    }

    public static boolean isNotFound(Exception e) {
        return isStatus(e, 404);
    }

    private static boolean isStatus(Exception e, int code) {
        return e instanceof UnexpectedHttpStatusException && ((UnexpectedHttpStatusException) e).getStatusCode() == code;
    }
}
