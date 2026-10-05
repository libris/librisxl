package whelk.util;

import org.apache.jena.irix.IRIException;
import org.apache.jena.irix.IRIx;

public class Iris {
    private Iris() {}

    public static boolean isBroken(String iriString) {
        try {
            return IRIx.create(iriString).hasViolations();
        } catch (IRIException e) {
            return true;
        }
    }
}
