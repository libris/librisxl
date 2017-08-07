package se.kb.libris.util.charcomposer;

import java.util.*;
import se.kb.libris.util.marc.*;
import se.kb.libris.util.marc.io.*;


/**
 *
 * @author marma
 */
public class MarcLatin1Strip {
    public static void main(String args[]) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: java se.kb.libris.util.charcomposer.MarcLatin1Strip <inencoding> <outencoding>");
            System.exit(1);
        }

        String fromEncoding = args[0], toEncoding = args[1];
        StrictIso2709Reader reader = new StrictIso2709Reader(System.in);
        StringBuffer sb = new StringBuffer(100*1000);
        byte record[] = null;
            
        while ((record = reader.readIso2709()) != null) {
            sb.setLength(0);
            String str = new String(record, fromEncoding);

            for (int i=0;i<str.length();i++) {
                int c = str.charAt(i);

                if (c > 0x00ff) {
                } else {
                    sb.append((char)c);
                }
            }
            
            MarcRecord mr = Iso2709Deserializer.deserialize(sb.toString().getBytes("UTF-8"), "UTF-8");
            System.out.write(Iso2709Serializer.serialize(mr, toEncoding));                    
        }
    }
}