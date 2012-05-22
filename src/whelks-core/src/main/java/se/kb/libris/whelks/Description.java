package se.kb.libris.whelks;

import java.io.InputStream;
import java.io.Reader;

public interface Description {
    public String getContentType();
    public String getFormat();
    public String getProfile();
    public String getData();
    public Reader getDataAsStream();
}
