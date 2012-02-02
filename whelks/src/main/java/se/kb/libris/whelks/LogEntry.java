/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kb.libris.whelks;

import java.net.URI;
import java.util.Date;
import java.util.Map;

/**
 *
 * @author marma
 */
public interface LogEntry {
    public URI getIdentifier();
    public Date getTimestamp();
    public Map<String, String> getInfo();
}
