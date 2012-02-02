/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kb.libris.whelks.plugin;

import java.util.List;
import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.Link;

/**
 *
 * @author marma
 */
public interface LinkFinder extends Plugin {
    public List<Link> findLinks(Document d);
}
