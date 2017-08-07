package se.kb.libris.util.charcomposer;

import java.util.*;
import se.kb.libris.util.marc.*;

public class ComposeUtil {
    public static void compose(MarcRecord mr, boolean compat) {
        Iterator iter = mr.iterator();
        
        while (iter.hasNext()) {
            Field f = (Field)iter.next();
            
            if (f instanceof Controlfield) {
                Controlfield cf = (Controlfield)f;
                cf.setData(com.ibm.icu.text.Normalizer.compose(cf.getData(), compat));
            } else {
                Datafield df = (Datafield)f;
                Iterator siter = df.iterator();
                
                while (siter.hasNext()) {
                    Subfield sf = (Subfield)siter.next();
                    sf.setData(compose(sf.getData(), compat));
                }
            }
        }
    }
    
    public static void decompose(MarcRecord mr, boolean compat) {
        Iterator iter = mr.iterator();
        
        while (iter.hasNext()) {
            Field f = (Field)iter.next();
            
            if (f instanceof Controlfield) {
                Controlfield cf = (Controlfield)f;
                cf.setData(com.ibm.icu.text.Normalizer.decompose(cf.getData(), compat));
            } else {
                Datafield df = (Datafield)f;
                Iterator siter = df.iterator();
                
                while (siter.hasNext()) {
                    Subfield sf = (Subfield)siter.next();
                    sf.setData(decompose(sf.getData(), compat));
                }
            }
        }
    }

    public static String compose(String str, boolean compat) {
        return com.ibm.icu.text.Normalizer.compose(str, compat);
    }
    
    public static String decompose(String str, boolean compat) {
        return com.ibm.icu.text.Normalizer.decompose(str, compat);
    }    
}
