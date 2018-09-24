package se.kb.libris.util.charcomposer;

import java.util.*;
import se.kb.libris.util.marc.*;
import se.kb.libris.util.marc.io.*;


/**
 *
 * @author marma
 */
public class MarcLatin1Compat {
    public static void main(String args[]) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: java se.kb.libris.util.charcomposer.MarcLatin1Compat <inencoding> <outencoding>");
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
                    if (c  == 0x0141) sb.append("L");       // POLISH L
                    else if (c  == 0x0142) sb.append("l");  // POLISH LOWERCASE L
                    else if (c  == 0x0152) sb.append("OE"); // OE DIGRAPH
                    else if (c  == 0x0153) sb.append("oe"); // LOWERCASE OE DIGRAPH
                    else if (c  == 0x0130) sb.append("I");  // DOTTED I
                    else if (c  == 0x0131) sb.append("i");  // DOTLESS LOWERCASE I
                    //else if (c  == 0x03BB) sb.append("L");  // LAMBDA
                    else if (c  == 0x0110) sb.append("D");  // D WITH CROSSBAR
                    else if (c  == 0x0111) sb.append("d");  // LOWERCASE D WITH CROSSBAR
                    else if (c  == 0x266D) sb.append("b");  // MUSIC FLAT SIGN
                    else if (c  == 0x266F) sb.append("#");  // MUSIC SHARP SIGN
                    else sb.append((char)c);
                } else {
                    sb.append((char)c);
                }
            }
            
            MarcRecord mr = Iso2709Deserializer.deserialize(sb.toString().getBytes("UTF-8"), "UTF-8");
            System.out.write(Iso2709Serializer.serialize(mr, toEncoding));        
        }
    }
}