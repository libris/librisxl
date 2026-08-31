package se.kb.libris.digi;

import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;

class RequestException extends RuntimeException {
    final int code;
    final String msg;

    RequestException(int code) {
        this(code, null);
    }

    RequestException(int code, String msg) {
        super(msg);
        this.code = code;
        this.msg = msg;
    }

    static RequestException badRequest(String msg) {
        return new RequestException(SC_BAD_REQUEST, msg);
    }

    static RequestException internalError(String msg) {
        return new RequestException(SC_INTERNAL_SERVER_ERROR, msg);
    }
}
