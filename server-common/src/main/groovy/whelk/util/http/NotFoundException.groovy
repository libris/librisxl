package whelk.util.http

class NotFoundException extends NoStackTraceException {
    NotFoundException(String msg) {
        super(msg)
    }
}
