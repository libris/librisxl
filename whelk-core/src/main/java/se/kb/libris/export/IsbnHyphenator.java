package se.kb.libris.export;

import se.kb.libris.util.marc.Datafield;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.Subfield;

import java.util.Iterator;

public class IsbnHyphenator {
    public static MarcRecord dehyphenate(MarcRecord rec) {
        Iterator iter = rec.getFields("020").iterator();

        while (iter.hasNext()) {
            Iterator siter = ((Datafield)iter.next()).iterator("a|z");
            
            while (siter.hasNext()) {
                Subfield sf = (Subfield)siter.next();
                
                // only process 020$z when "print" or "online"
                if (sf.getCode() == 'z' && !(sf.getData().contains("print") || sf.getData().contains("online"))) continue;
                
                String data = sf.getData(), isbn = "";

                int i;
                for (i=0;i<data.length();i++) {
                    char c = data.charAt(i);
                    
                    if (Character.isDigit(c) || c == 'x' || c == 'X') {
                        isbn += c;
                    } else if (c == '-') {
                    } else {
                        break;
                    }
                }                
                    
                if (isbn.length() == 10 || isbn.length() == 13) {
                    sf.setData(isbn + data.substring(i));
                }
            }
        }
        
        return rec;
    }
    
    public static MarcRecord hyphenate(MarcRecord rec) {        
        Iterator iter = rec.getFields("020").iterator();
        
        while (iter.hasNext()) {
            Iterator siter = ((Datafield)iter.next()).iterator("a|z");
            
            while (siter.hasNext()) {
                Subfield sf = (Subfield)siter.next();
                
                // only process 020$z when "print" or "online"
                if (sf.getCode() == 'z' && !(sf.getData().toLowerCase().contains("print") || sf.getData().toLowerCase().contains("online"))) continue;

                String data = sf.getData(), isbn = "";
                
                int i;
                for (i=0;i<data.length();i++) {
                    char c = data.charAt(i);
                    
                    if (Character.isDigit(c) || c == 'x' || c == 'X') {
                        isbn += c;
                    } else if (c == '-') {
                    } else {
                        break;
                    }
                }                
                    
                if (isbn.length() == 10 || isbn.length() == 13) {
                    try {
                        sf.setData(se.kb.libris.utils.isbn.IsbnParser.parse(isbn).toString(true) + data.substring(i));
//                        System.out.println(isbn + " --> " + se.kb.libris.utils.isbn.IsbnParser.parse(isbn).toString(true));
                    } catch (Exception e) {
//                        System.err.println(e.getMessage());
//                        e.printStackTrace(System.err);
                    }
                }
            }
        }
        
        return rec;
    }    
}
