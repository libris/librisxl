/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kb.libris.whelks.index;

import se.kb.libris.whelks.exception.WhelkException;
import se.kb.libris.whelks.index.Query;
import se.kb.libris.whelks.index.SearchResult;

/**
 *
 * @author marma
 */
public interface Searchable {
        public SearchResult find(Query query) throws WhelkException;
}
