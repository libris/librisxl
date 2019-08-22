package se.kb.libris.util.charcomposer;

import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.io.Iso2709Deserializer;
import se.kb.libris.util.marc.io.Iso2709Serializer;
import se.kb.libris.util.marc.io.StrictIso2709Reader;


/**
 *
 * @author marma
 */
public class MarcDecompose {
    public static void main(String args[]) throws Exception {
        if (args.length != 2 && args.length != 4) {
            System.err.println("usage: java se.kb.libris.util.charcomposer.MarcDecompose <inencoding> <outencoding> [skipfrom skipto]");
            System.exit(1);
        }
        
        String fromEncoding = args[0], toEncoding = args[1];
        StrictIso2709Reader reader = new StrictIso2709Reader(System.in);
        byte record[] = null;

        if (args.length == 4) {
            StringBuffer sb = new StringBuffer(100*1000);
            int from = Integer.parseInt(args[2], 16);
            int to = Integer.parseInt(args[3], 16);
            
            while ((record = reader.readIso2709()) != null) {
                sb.setLength(0);
                String str = new String(record, fromEncoding);
                
                for (int i=0;i<str.length();i++) {
                    int c = str.charAt(i);
                    
                    if (c  >= from && c <= to) sb.append((char)c);
                    else sb.append(com.ibm.icu.text.Normalizer.decompose(String.valueOf((char)c), false));
                }
                
                MarcRecord mr = Iso2709Deserializer.deserialize(sb.toString().getBytes("UTF-8"), "UTF-8");
                System.out.write(Iso2709Serializer.serialize(mr, toEncoding));
            }
        } else {
            while ((record = reader.readIso2709()) != null) {
                MarcRecord mr = Iso2709Deserializer.deserialize(record, fromEncoding);
                ComposeUtil.decompose(mr, false);
                System.out.write(Iso2709Serializer.serialize(mr, toEncoding));
            }
        }
    }
}
