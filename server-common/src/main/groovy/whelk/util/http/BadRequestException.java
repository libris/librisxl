package whelk.util.http;

import java.util.Collections;
import java.util.Map;

public class BadRequestException extends NoStackTraceException {
    private Map<String, Object> extraInfo;

    public BadRequestException(String msg) {
        this(msg, null);
    }

    public BadRequestException(String msg, Map<String, Object> extraInfo) {
        super(msg);
        this.extraInfo = extraInfo;
    }

    public Map<String, Object> getExtraInfo() {
        return extraInfo != null ? extraInfo : Collections.emptyMap();
    }
}