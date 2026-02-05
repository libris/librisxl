import java.io.*;
import java.util.*;
import static java.lang.System.out;

import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.io.Iso2709MarcRecordReader;

import static whelk.util.Jackson.mapper;
import whelk.converter.MarcJSONConverter;
import whelk.converter.marc.MarcFrameConverter;
import whelk.converter.marc.MarcFrameCli;

// Example usage:
// $ java -cp importers/build/libs/xlimporter.jar -Dxl.secret.properties=DEV2-secret.properties librisxl-tools/scripts/ConvertSHB.java ~/Downloads/shb_alfanum_marc > /var/tmp/shb_alnum.jsonld.lines
public class ConvertSHB {
  public static void main(String[] args) throws Exception {
    var converter = new MarcFrameConverter();
    new MarcFrameCli().addSystemComponents(converter);

    var reader = new Iso2709MarcRecordReader(MarcJSONConverter.getNormalizedInputStreamFromFile(new File(args[0])));
    MarcRecord record = null;
    int i = 0;
    while (true) {
      record = reader.readRecord();
      if (record == null)
        break;

      String id = "dataset/shb/" + (++i);
      var result = (Map) converter.convert(MarcJSONConverter.toJSONMap(record), id);
      out.println(mapper.writeValueAsString(result));
    }
  }
}
