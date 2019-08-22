package se.kb.libris.util.charcomposer;

import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.io.Iso2709Deserializer;
import se.kb.libris.util.marc.io.Iso2709Serializer;
import se.kb.libris.util.marc.io.StrictIso2709Reader;


/**
 *
 * @author marma
 */
public class MarcCompose {
    public static void main(String args[]) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: java se.kb.libris.util.charcomposer.MarcCompose <inencoding> <outencoding>");
            System.exit(1);
        }
        
        String fromEncoding = args[0], toEncoding = args[1];
        StrictIso2709Reader reader = new StrictIso2709Reader(System.in);
        byte record[] = null;

        while ((record = reader.readIso2709()) != null) {
            MarcRecord mr = Iso2709Deserializer.deserialize(record, fromEncoding);
            ComposeUtil.compose(mr, false);
            System.out.write(Iso2709Serializer.serialize(mr, toEncoding));
        }
    }
}
