/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package se.kb.libris.export.dewey;

/**
 *
 * @author marma
 */
public enum Usage {
    MAIN, LEFT, RIGHT, UNKNOWN;

    static Usage getUsage(String[] flags) {
        for (String f: flags) {
            if (f.equals("#H")) return MAIN;
            else if (f.equals("#G")) return RIGHT;
            else if (f.equals("#M")) return LEFT;
        }

        return UNKNOWN;
    }
}
