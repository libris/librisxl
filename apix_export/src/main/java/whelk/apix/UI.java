package whelk.apix;

import java.time.ZonedDateTime;

public interface UI
{
    void outputText(String text);
    void setCurrentTimeStamp(ZonedDateTime zdt);
}
