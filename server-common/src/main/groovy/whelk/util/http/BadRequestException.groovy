package whelk.util.http

class BadRequestException extends NoStackTraceException {
    Map extraInfo
    BadRequestException(String msg, Map extraInfo = null) {
        super(msg)
        this.extraInfo = extraInfo
    }
    
    Map getExtraInfo() {
        return extraInfo ?: Collections.EMPTY_MAP
    }
}