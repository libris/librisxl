package whelk.apixserver;

import org.apache.commons.io.IOUtils;
import se.kb.libris.util.marc.*;
import se.kb.libris.util.marc.io.MarcXmlRecordReader;
import se.kb.libris.util.marc.io.MarcXmlRecordWriter;
import whelk.Document;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.ListIterator;

/**
 * The following code has been transcribed from
 * https://git.kb.se/libris_legacy/apix/blob/master/src/main/java/se/kb/libris/apix/cat/RecordResource_0_1.java
 * with minimal changes.
 */
public class Digidaily {
    static void saveDigiDaily (HttpServletRequest request, HttpServletResponse response) throws Exception
    {
        String[] parameters = Utils.getPathSegmentParameters(request);

        String operation = parameters[2];
        if (!operation.equals("importdigikb"))
        {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        String content = IOUtils.toString(request.getReader());

        StringWriter out = new StringWriter();

        for (String line: content.split("\n")) {
            if (line.charAt(0) == '#') continue;

            /** @todo stricter checking **/
            String s[] = line.split("\t"), bibid = s[0], sigel = s[1], f0429 = s[2], work_url = s[3], work_text = s[4], img_url = s[5], license = s[6], place = s[7], agency = s[8], non_public_note = s[9], img_public_note = s[10];

            // hämta tryck
            Document r = Utils.getXlDocument(bibid, "bib");

            if (r == null) {
                out.write("B" + bibid + "\tWARNING NO SUCH RECORD\n");
                continue;
            }

            String rMarcXml = Utils.convertToMarcXml(r);

            InputStream marcXmlInputStream = new ByteArrayInputStream(rMarcXml.getBytes("UTF-8"));
            MarcXmlRecordReader reader = new MarcXmlRecordReader(marcXmlInputStream, "/record");
            MarcRecord printRecord = reader.readRecord();

            // dublettkoll
            // gå igenom 776 och leta efter bibid i $w, kolla om den posten har en beståndspost för sigel S (852$b = 'S')
            Document r2 = null;
            String mfhd_id = null;
            String ebib_id = null;
            boolean newbib = true;
            for (Datafield df: printRecord.getDatafields("776")) {
                for (Subfield sf: df.getSubfields("w")) {
                    Document dup = Utils.getXlDocument(sf.getData(), "bib");
                    List<Document> attachedHoldings = Utils.s_whelk.getAttachedHoldings(dup.getThingIdentifiers());
                    for (Document attachedHolding : attachedHoldings) {
                        if (attachedHolding.getSigel().equals(sigel)) {
                            if (sf.getData() != null) {
                                out.write("B" + bibid + "\tWARNING MULTIPLE DIGI RECORDS\n");
                                continue;
                            }

                            ebib_id = sf.getData();
                            r2 = dup;
                            mfhd_id = attachedHolding.getShortId();
                            newbib = false;
                        }
                    }
                }
            }

            // skapa eRecord
            MarcRecord eRecord = MarcXmlRecordReader.fromXml(rMarcXml);
            // något med eRecord ...
            eRecord.addField(eRecord.createControlfield("007", "cr||||||||||||"));
            int position_008 = 23;
            for (Controlfield cf : eRecord.getControlfields("000")) {
                if (cf.getChar(6) == 'f' || cf.getChar(6) == 'g') {
                    position_008 = 29;
                }
            }
            for (Controlfield cf : eRecord.getControlfields("008")) {
                cf.setChar(position_008, 'o');
            }
            // Flytta ISBN etc.
            for (Datafield df : eRecord.getDatafields()) {
                if (df.getTag().equals("020") || df.getTag().equals("022") || df.getTag().equals("024")) {
                    ListIterator<Subfield> sfields = (ListIterator<Subfield>) df.listIterator();
                    while (sfields.hasNext()) {
                        Subfield sf = sfields.next();
                        if (sf.getCode() == 'a') {
                            df.addSubfield('z', sf.getData() + " (Print)");
                            sfields.remove();
                        }
                    }
                }

            }

            // Ta bort 035 och 040
            ListIterator<Field> fields = (ListIterator<Field>) eRecord.listIterator();
            while (fields.hasNext()) {
                Field f = fields.next();
                if (f.getTag().equals("035") || f.getTag().equals("040")) {
                    fields.remove();
                }
            }

            // Lägg till nytt 040
            eRecord.addField(eRecord.createDatafield("040").addSubfield('a', sigel));

            // Ta bort 042 med NB
            fields = (ListIterator<Field>) eRecord.listIterator();
            while (fields.hasNext()) {
                Field f = fields.next();
                if (f.getTag().equals("042")) {
                    for (Subfield sf : ((Datafield)f).getSubfields()) {
                        if (sf.getCode() == '9' && sf.getData().equals("NB")) {
                            fields.remove();
                        }
                    }
                }
            }
            // Nytt 042_9
            eRecord.addField(eRecord.createDatafield("042").addSubfield('9', "DIGI"));
            if (f0429.length() > 0) {
                eRecord.addField(eRecord.createDatafield("042").addSubfield('9', f0429));
            }

            // Lägg till 533
            GregorianCalendar gc = new GregorianCalendar();
            String year = "" + gc.get(Calendar.YEAR);
            eRecord.addField(eRecord.createDatafield("533").addSubfield('a', "Digitalt faksimil och/eller elektronisk text").addSubfield('b', place).addSubfield('c', agency).addSubfield('d', year));

            // Lägg till 540
            if (license.equals("PDM")) {
                eRecord.addField(eRecord.createDatafield("540").addSubfield('a', "PDM 1.0").addSubfield('u', "http://creativecommons.org/publicdomain/mark/1.0/"));
            } else if (license.equals("CC0")) {
                eRecord.addField(eRecord.createDatafield("540").addSubfield('a', "CC0 1.0").addSubfield('u', "https://creativecommons.org/publicdomain/zero/1.0/"));
            }

            // 776
            Datafield df776 = eRecord.createDatafield("776")
                    .setIndicator(0, '0')
                    .setIndicator(1, '8')
                    .addSubfield('i', "Orginalversion:");

            addSubfieldIfExists(df776, 'a', getSubfieldDataAsString(eRecord, "100|110|111|130", "a"));
            addSubfieldIfExists(df776, 't', getSubfieldDataAsString(eRecord, "222|245", null));
            addSubfieldIfExists(df776, 'd', getSubfieldDataAsString(eRecord, "260", "c"));
            addSubfieldIfExists(df776, 'x', getSubfieldDataAsString(printRecord, "020", "a"));
            addSubfieldIfExists(df776, 'z', getSubfieldDataAsString(printRecord, "022", "a"));

            df776.addSubfield('w', bibid);

            // clear existing 776s
            ListIterator iter= eRecord.listIterator();
            while (iter.hasNext()) {
                Field f = (Field)iter.next();

                if (f.getTag().equals("776")) {
                    Datafield df = (Datafield)f;

                    for (Subfield sf: df.getSubfields("w")) {
                        // rensa även felaktiga 776 som pekar på den egna posten
                        if (sf.getData().equals(bibid) || (ebib_id != null && sf.getData().equals(ebib_id))) {
                            iter.remove();
                            break;
                        }
                    }
                }
            }

            eRecord.addField(df776, MarcFieldComparator.strictSorted);

            // lägg till info om att det är en elektronisk resurs *efter* att 776 om print skapats
            for (Datafield df : eRecord.getDatafields("245")) {
                df.addSubfield('h', "[Elektronisk resurs]");
            }

            // 856
            eRecord.addField(eRecord
                    .createDatafield("856")
                    .setIndicator(0, '4')
                    .setIndicator(1, '0')
                    .addSubfield('u', work_url)
                    .addSubfield('x', "digiwork")
                    .addSubfield('z', work_text));

            if (img_url.length() > 0) {
                eRecord.addField(eRecord
                        .createDatafield("856")
                        .setIndicator(0, '4')
                        .setIndicator(1, '2')
                        .addSubfield('u', img_url)
                        .addSubfield('x', "digipic")
                        .addSubfield('z', img_public_note));
            }


            // Konvertera och spara eRecord
            Document bibToBeSaved;
            {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                MarcXmlRecordWriter marcXmlRecordWriter = new MarcXmlRecordWriter(baos);
                marcXmlRecordWriter.writeRecord(eRecord);
                if (newbib) {
                    bibToBeSaved = Utils.convertToRDF(baos.toString("UTF-8"), "bib", null, false);
                    Utils.s_whelk.createDocument(bibToBeSaved, Utils.APIX_SYSTEM_CODE, request.getRemoteUser(), "bib", false);
                    out.write(bibToBeSaved.getShortId() + "\tDIGI CREATED\n");
                } else {
                    bibToBeSaved = Utils.convertToRDF(baos.toString("UTF-8"), "bib", null, true);
                    Utils.s_whelk.storeAtomicUpdate(bibToBeSaved.getShortId(), false, Utils.APIX_SYSTEM_CODE, request.getRemoteUser(),
                            (Document doc) -> {
                                doc.data = bibToBeSaved.data;
                            });
                    out.write(bibToBeSaved.getShortId() + "\tDIGI UPDATED\n");
                }
            }


            // skapa beståndspost
            if (mfhd_id == null) {
                MarcRecordBuilder builder = MarcRecordBuilderFactory.newBuilder();
                MarcRecord holdRecord = builder.createMarcRecord();
                java.util.Date date= new java.util.Date();
                Format sdf = new SimpleDateFormat("yyMMdd");
                String timestamp = sdf.format(date);
                String val008 = timestamp + "||0000|||||001||||||000000";
                holdRecord.setLeader("     nx  a22     1n 4500");
                holdRecord.addField(holdRecord.createControlfield("004", bibToBeSaved.getShortId()));
                holdRecord.addField(holdRecord.createControlfield("008", val008));
                holdRecord.addField(holdRecord
                        .createDatafield("852")
                        .setIndicator(0, ' ')
                        .setIndicator(1, ' ')
                        .addSubfield('b', sigel)
                        .addSubfield('x', non_public_note)
                        .addSubfield('z', "Digitaliserat exemplar")
                        .addSubfield('z', "Fritt tillgängligt via internet"));
                // skapa ny beståndspost
                // lägg till beståndspost
                {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    MarcXmlRecordWriter marcXmlRecordWriter = new MarcXmlRecordWriter(baos);
                    marcXmlRecordWriter.writeRecord(eRecord);
                    Document holdToBeSaved = Utils.convertToRDF(baos.toString("UTF-8"), "hold", null, false);
                    Utils.s_whelk.createDocument(holdToBeSaved, Utils.APIX_SYSTEM_CODE, request.getRemoteUser(), "hold", false);
                    out.write(holdToBeSaved.getShortId() + "\tMFHD CREATED\n");
                }
            }

            if (newbib) {
                String digid = r2.getShortId();
                Datafield df776_2 = printRecord.createDatafield("776")
                        .setIndicator(0, '0')
                        .setIndicator(1, '8')
                        .addSubfield('i', "Digitaliserad version:");
                addSubfieldIfExists(df776_2, 'a', getSubfieldDataAsString(eRecord, "100|110|111|130", "a"));
                addSubfieldIfExists(df776_2, 't', getSubfieldDataAsString(eRecord, "222|245", null));
                addSubfieldIfExists(df776_2, 'd', getSubfieldDataAsString(eRecord, "260", "c"));
                addSubfieldIfExists(df776_2, 'x', getSubfieldDataAsString(printRecord, "020", "a"));
                addSubfieldIfExists(df776_2, 'z', getSubfieldDataAsString(printRecord, "022", "a"));
                df776_2.addSubfield('w', digid);
                printRecord.addField(df776_2);

                // lägg till länk från tryck till digi

                // något med printRecord ...

                // spara tryck
                {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    MarcXmlRecordWriter marcXmlRecordWriter = new MarcXmlRecordWriter(baos);
                    marcXmlRecordWriter.writeRecord(eRecord);
                    Document printBibToBeSaved = Utils.convertToRDF(baos.toString("UTF-8"), "bib", null, true);
                    Utils.s_whelk.storeAtomicUpdate(printBibToBeSaved.getShortId(), false, Utils.APIX_SYSTEM_CODE, request.getRemoteUser(),
                            (Document doc) -> {
                                doc.data = printBibToBeSaved.data;
                            });
                    out.write(printBibToBeSaved.getShortId() + "\tPRINT UPDATED\n");
                }
            }
        }

        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("text/plain");
        ServletOutputStream outStream = response.getOutputStream();
        outStream.print(out.toString());
        out.close();
    }

    private static Datafield addSubfieldIfExists(Datafield df, char subfield, String value) {
        if (value != null) {
            df.addSubfield(subfield, value);
        }
        return df;
    }

    private static String getSubfieldDataAsString(MarcRecord record, String tags, String subfields) {
        StringBuilder sb = new StringBuilder();
        for (Datafield df : record.getDatafields(tags)) {
            if (subfields != null) {
                for (Subfield sf : df.getSubfields(subfields)) {
                    return sf.getData();
                }
            } else {
                for (Subfield sf : df.getSubfields()) {
                    sb.append(sf.getData() + " ");
                }
                return sb.toString().trim();
            }
        }
        return null;
    }
}
