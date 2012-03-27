package se.kb.libris.whelks.api.http;

import java.io.InputStream;
import javax.servlet.http.HttpServletRequest;

public interface RawHttpAccessible {
    public InputStream raw(HttpServletRequest req);
}
