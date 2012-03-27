/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kb.libris.whelks.graph;

import java.io.InputStream;

/**
 *
 * @author marma
 */
public interface SparqlEndpoint {
    public InputStream sparql(String query) throws SparqlException;    
}
