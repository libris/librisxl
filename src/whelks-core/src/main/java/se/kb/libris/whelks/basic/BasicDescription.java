package se.kb.libris.whelks.basic;

import java.io.Reader;
import java.io.StringReader;
import se.kb.libris.whelks.Description;

public class BasicDescription implements Description {
    private String contentType = null, format = null, profile = null;
    String data;
    
    public BasicDescription(String _contentType, String _format, String _profile, String _data) {
        contentType = _contentType;
        format = _format;
        profile = _profile;
        data = _data;
    }
    
    @Override
    public String getContentType() {
        return contentType;
    }

    @Override
    public String getFormat() {
        return format;
    }

    @Override
    public String getProfile() {
        return profile;
    }

    @Override
    public String getData() {
        return data;
    }

    @Override
    public Reader getDataAsStream() {
        return new StringReader(data);
    }
}
