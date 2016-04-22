package whelk.importer;

import java.util.*;
import se.kb.libris.util.marc.*;

public class DigId {
    public static String getControlFieldData(MarcRecord mr, String tag) {
        Iterator iter = mr.iterator(tag);

        while (iter.hasNext()) {
            Controlfield cf = (Controlfield)iter.next();

            return cf.getData();
        }

        return null;
    }

    public static String getSubfieldData(Datafield df, char code) {
        Iterator iter = df.iterator(String.valueOf(code));
        
        if (iter.hasNext()) {
            return ((Subfield)iter.next()).getData();
        } else {
            return null;
        }
    }
    
    public static String grepIsbna(Datafield df) {
        String data = getSubfieldData(df, 'a');
        String ret = "";

        if (data == null) return null;

        for (int i=0;i<data.length();i++) {
            char c = data.charAt(i);

            if (c >= '0' && c <= '9') {
                ret += c;
            } else if (c == 'x' || c == 'X') {
                ret += 'X';
            } else if (c == '-') {
            } else break;
        }

        if (ret.length() == 10 || ret.length() == 13) {
            return ret;
        } else {
            return null;
        }
    }

    public static String grepLibrisId(MarcRecord mr) {
        String f003 = getControlFieldData(mr, "003"), librisId = null;

        if (f003 != null && (f003.equals("SE-LIBR") || f003.equals("LIBRIS"))) {
            return getControlFieldData(mr, "001");
        }

        Iterator iter = mr.iterator("035");
        while (iter.hasNext()) {
            Datafield df = (Datafield)iter.next();
            String data = getSubfieldData(df, 'a');

            if (data == null) return null;

            if (data.startsWith("(SE-LIBR)")) {
                return data.substring(9).trim();
            }

            if (data.startsWith("(LIBRIS)")) {
                return data.substring(8).trim();
            }
        }

        return null;
    }

    public static String grepIsbnz(Datafield df) {
        String data = getSubfieldData(df, 'z');
        String ret = "";
        
        if (data == null) return null;
        
        for (int i=0;i<data.length();i++) {
            char c = data.charAt(i);
            
            if (c >= '0' && c <= '9') {
                ret += c;
            } else if (c == 'x' || c == 'X') {
                ret += 'X';
            } else if (c == '-') {
            } else break;
        }
        
        if (ret.length() == 10 || ret.length() == 13) {
            return ret;
        } else {
            return null;
        }
    }
    
    public static String[] grepIssn(Datafield df) {
        String a = grepIssn(df, 'a');
        String z = grepIssn(df, 'z');
        
        if (a != null && z != null) {
            String ret[] = new String[2];
            ret[0] = a;
            ret[1] = z;
            
            return ret;
        } else if (a != null) {
            String ret[] = new String[1];
            ret[0] = a;
            
            return ret;
        } else if (z != null) {
            String ret[] = new String[1];
            ret[0] = z;
            
            return ret;
        }
        
        return new String[0];
    }
    
    public static String grepIssn(Datafield df, char code) {
        String data = getSubfieldData(df, code);
        String ret = "";
        
        if (data == null) return null;
        
        for (int i=0;i<data.length();i++) {
            char c = data.charAt(i);
            
            if (c >= '0' && c <= '9') {
                ret += c;
            } else if (c == '-') {
                ret += c;
            } else if (c == 'x' || c == 'X') {
                ret += 'X';
            } else break;
        }

        if ( ret.length() != 9 && ret.length() != 8 ) {
            ret = null;
        } // KP 151126
        
        return ret;
    }
    
    public static  String grepUrn(Datafield df) {
        String data = getSubfieldData(df, 'a');
        
	if (data == null) return null;

        if (data.indexOf("(") != -1) {
            data = data.substring(data.indexOf("(")).trim();
        }
        
        if (data.startsWith("urn:") || data.startsWith("URN:")) {
            return data;
        } else {
            return null;
        }
    }
    
    public static String grepOaiId(Datafield df) {
        String data = getSubfieldData(df, 'a');

	if (data == null) return null;
        
        if (data.indexOf("(") != -1) {
            data = data.substring(data.indexOf("(")).trim();
        }
        
        if (data.startsWith("oai:") || data.startsWith("OAI:")) {
            return data;
        } else {
            return null;
        }
    }
    
    public static String grep035a(Datafield df) {
        String data = getSubfieldData(df, 'a');

	return data;
    }
    
    public static String[][] digIds(MarcRecord mr) {
        Iterator iter = mr.iterator("020|022|035|856");
        Vector<String> v = new Vector<String>();
        
        while (iter.hasNext()) {
            Datafield df = (Datafield)iter.next();
            
            if (df.getTag().equals("020")) {                
                String isbna = grepIsbna(df);                
                if (isbna != null) v.add("ISBNA" + ' ' + isbna);
                String isbnz = grepIsbnz(df);                
                if (isbnz != null) v.add("ISBNZ" + ' ' + isbnz);
            } else if (df.getTag().equals("022")) {
                for (String i: grepIssn(df)) {
                    v.add("ISSN" + ' ' + i);
                }
                
                String issna = grepIssn(df, 'a');
                String issnz = grepIssn(df, 'z');

                if (issna !=  null) v.add("ISNA" + ' ' + issna);
                if (issnz !=  null) v.add("ISNZ" + ' ' + issnz);
            } else if (df.getTag().equals("035")) {
                String urn = grepUrn(df);
                String oai = grepOaiId(df);
                String a035 = grep035a(df);
                
                if (urn != null) v.add("URN" + ' ' + urn);                                        
                if (oai != null) v.add("OAI" + ' ' + oai);                                        
                if (a035 != null) v.add("035A" + ' ' + a035);
            }
        }
        
        String librisId = grepLibrisId(mr);
        if (librisId != null) v.add("LIBRIS-ID" + ' ' + librisId);

        if (v.size() != 0) {
            String ret[][] = new String[v.size()][];
            
            for (int i=0;i<v.size();i++) {
                ret[i] = v.get(i).split(" ");
            }
            
            return ret;
        } else {
            return null;
        }
    }
}
