/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kb.libris.whelks.backends.riak;

import java.net.URI;
import se.kb.libris.whelks.Key;

/**
 *
 * @author marma
 */
public class RiakKey implements Key {
    URI type;
    String value;
    
    @Override
    public URI getType() {
        return type;
    }

    @Override
    public String getValue() {
        return value;
    }
}
