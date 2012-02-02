/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kb.libris.whelks.plugin;

import java.util.List;
import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.Key;

/**
 *
 * @author marma
 */
public interface KeyGenerator extends Plugin {
    List<? extends Key> generateKeys(Document d);
}
