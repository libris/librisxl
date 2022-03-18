package whelk.rest.api

class BadRequestException extends RuntimeException {
    Object extraInfo // TODO: use
    BadRequestException(String msg, Object extraInfo = null) {
        super(msg)
        this.extraInfo = extraInfo
    }
}