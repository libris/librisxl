package se.kb.libris.util.charcomposer;

import java.io.*;
import java.util.*;
import se.kb.libris.util.marc.*;
import se.kb.libris.util.marc.io.*;


/**
 *
 * @author marma
 */
public class MarcLatin1Compat2 {
    public static void main(String args[]) throws Exception {
        if (args.length != 3) {
            System.err.println("usage: java se.kb.libris.util.charcomposer.MarcLatin1Compat <inencoding> <outencoding> <file>");
            System.exit(1);
        }
        
        String fromEncoding = args[0], toEncoding = args[1];
        StrictIso2709Reader reader = new StrictIso2709Reader(System.in);
        StringBuffer sb = new StringBuffer(100*1000);
        byte record[] = null;
        String map[] = new String[65536];
        
        for (int i=0;i<65536;i++) {
            map[i] = String.valueOf((char)i);
        }
        
        BufferedReader breader = new BufferedReader(new FileReader(args[2]));
        String line=null;
        
        while ((line = breader.readLine()) != null) {
            if (line.length() == 0 || line.charAt(0) == '#') continue;
            
            if (line.indexOf(' ') != -1) line = line.substring(0, line.indexOf(' '));
            if (line.indexOf('\t') != -1) line = line.substring(0, line.indexOf('\t'));
            
            if (line.indexOf('=') != -1) {
                String from = line.substring(0, line.indexOf('='));
                String to = line.substring(line.indexOf('=')+1);
                
                System.err.println("mapping: " + from + " -> '" + to + "'");
                
                map[Integer.parseInt(from, 16)] = to;
                
            }
        }
        
            
        while ((record = reader.readIso2709()) != null) {
            sb.setLength(0);
            String str = new String(record, fromEncoding);

            for (int i=0;i<str.length();i++) {
                int c = str.charAt(i);

                if (c > 0x007f) {
                    sb.append(map[c]);
                } else {
                    sb.append((char)c);
                }
            }
            
            MarcRecord mr = Iso2709Deserializer.deserialize(sb.toString().getBytes("UTF-8"), "UTF-8");
            System.out.write(Iso2709Serializer.serialize(mr, toEncoding));        
        }
    }
}