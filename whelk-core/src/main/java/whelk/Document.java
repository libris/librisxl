package whelk;

import java.util.Map;

public interface Document {
    static final String TIMESTAMP_KEY = "timestamp";
    static final String MODIFIED_KEY = "modified";
    static final String CONTENT_TYPE_KEY = "contentType";

    public String getIdentifier();
    public void setIdentifier(String id);

    public String getContentType();
    public void setContentType(String contenttype);
    public long getTimestamp();
    public long getModified();
    public void setModified(long ts);
    public String getChecksum();
    public String getDataset();
    public int getVersion();
    public byte[] getData();

    public Document withData(byte [] data);
    public long updateModified();

    public Map<String,Object> getEntry();
    public void setEntry(Map<String, Object> entry);
    public Map<String,Object> getMeta();
    public void setMeta(Map<String, Object> entry);

    public String getMetadataAsJson();

    public boolean isJson();
}
