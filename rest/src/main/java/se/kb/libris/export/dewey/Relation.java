/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package se.kb.libris.export.dewey;

/**
 *
 * @author marma
 */
public enum Relation {
    EXACT, SUBSET, SUPERSET, OVERLAPPING, UNKNOWN;

    static Relation getRelation(String[] flags) {
        for (String f: flags) {
            if (f.equals("#1")) return EXACT;
            else if (f.equals("#2")) return SUBSET;
            else if (f.equals("#3")) return SUPERSET;
            else if (f.equals("#4")) return OVERLAPPING;
        }

        return UNKNOWN;
    }
}
