package se.kb.libris.whelks.api

class ApiResult {

    String result
    String contentType
    int statusCode = 200
    URL redirect

    ApiResult(int statusCode, String message = null) {
        this.statusCode = statusCode
        this.result = message
    }

    ApiResult(String r, String ct) {
        result = r
        contentType = ct
    }
}
