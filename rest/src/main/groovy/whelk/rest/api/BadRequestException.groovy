package whelk.rest.api

class BadRequestException extends RuntimeException {
    BadRequestException(String msg) {
        super(msg)
    }
}