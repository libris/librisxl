/*
 * Normalizer.java
 *
 * Created on May 28, 2007, 5:26 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package se.kb.libris.util.charcomposer;

/**
 *
 * @author marma
 */
public class Normalizer {
    public static int LATIN_1 = 0;
    public static int LATIN_1_COMPAT = 0;
    
    public static CharSequence compose(CharSequence seq) {
        return com.ibm.icu.text.Normalizer.compose(seq.toString(), false);
    }
    
    public static CharSequence decompose(CharSequence seq) {
        return com.ibm.icu.text.Normalizer.decompose(seq.toString(), false);
    }
    
    public static CharSequence decompose(char c) {
        return com.ibm.icu.text.Normalizer.decompose(String.valueOf(c), false);
    }    
    
    public static CharSequence normalize(CharSequence seq) {
        StringBuffer sb = new StringBuffer();
        
        seq = compose(decompose(seq));
        
        for (int i=0;i<seq.length();i++) {
            char c = seq.charAt(i);
            
            if (c > 0xff)
                sb.append(decompose(c));
            else
                sb.append(c);
        }
        
        return sb;
    }
}
