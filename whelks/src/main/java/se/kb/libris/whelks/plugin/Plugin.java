/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kb.libris.whelks.plugin;

/**
 *
 * @author marma
 */
public interface Plugin {
    public String getId();
    public String getName();
    public void enable();
    public void disable();
    public boolean isEnabled();
}
