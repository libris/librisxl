package whelk.rest.api

class BadRequestException extends Crud.NoStackTraceException {
    Map extraInfo
    BadRequestException(String msg, Map extraInfo = null) {
        super(msg)
        this.extraInfo = extraInfo
    }
    
    Map getExtraInfo() {
        return extraInfo ?: Collections.EMPTY_MAP
    }
}