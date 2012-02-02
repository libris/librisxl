/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kb.libris.whelks.temp;

import java.io.Serializable;
import java.util.LinkedList;

/**
 *
 * @author marma
 */
public class GenericsTest {
    public Iterable<? extends Serializable> a() {
        return new LinkedList<String>();
    }
}
