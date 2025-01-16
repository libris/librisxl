package whelk.util.http

class OtherStatusException extends NoStackTraceException {
    int code

    OtherStatusException(String msg, int code, Throwable cause = null) {
        super(msg, cause)
        this.code = code
    }
}
