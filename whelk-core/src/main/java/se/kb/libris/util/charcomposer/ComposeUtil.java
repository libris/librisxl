package se.kb.libris.util.charcomposer;

import se.kb.libris.util.marc.Controlfield;
import se.kb.libris.util.marc.Datafield;
import se.kb.libris.util.marc.Field;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.Subfield;

import java.text.Normalizer;
import java.util.Iterator;

public class ComposeUtil {
    public static void compose(MarcRecord mr, boolean compat) {
        Iterator iter = mr.iterator();
        
        while (iter.hasNext()) {
            Field f = (Field)iter.next();
            
            if (f instanceof Controlfield) {
                Controlfield cf = (Controlfield)f;
                cf.setData(compose(cf.getData(), compat));
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
                cf.setData(decompose(cf.getData(), compat));
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
        return compat 
                ? Normalizer.normalize(str, Normalizer.Form.NFKC)
                : Normalizer.normalize(str, Normalizer.Form.NFC);
    }
    
    public static String decompose(String str, boolean compat) {
        return compat
                ? Normalizer.normalize(str, Normalizer.Form.NFKD)
                : Normalizer.normalize(str, Normalizer.Form.NFD);
    }    
}
