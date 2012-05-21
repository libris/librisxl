/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package se.kb.libris.imports;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.io.MarcXmlRecordReader;
import se.kb.libris.util.marc.io.Iso2709Serializer;
import java.net.URL;
import java.net.URLConnection;
import org.apache.commons.codec.binary.Base64;
import se.kb.libris.conch.converter.MarcJSONConverter;
/**
 *
 * @author katarina
 */
public class BatchImportTest {

    public static void main(String[] args) throws Exception{
        MarcXmlRecordReader marcXmlRecordReader = null;
        try {
            URL url = new URL("http://data.libris.kb.se/auth/oaipmh/?verb=ListRecords&metadataPrefix=marcxml");
            String authString = "apibeta:beta";
            byte[] authEncBytes = Base64.encodeBase64(authString.getBytes());
            String authStringEnc = new String(authEncBytes);
            URLConnection urlConnection = url.openConnection();
            urlConnection.setRequestProperty("Authorization", "Basic " + authStringEnc);
            InputStream is = urlConnection.getInputStream();

            marcXmlRecordReader = new MarcXmlRecordReader(is, "/OAI-PMH/ListRecords/record/metadata/record");

            MarcRecord record;
            while ((record = marcXmlRecordReader.readRecord()) != null) {
                System.out.write(Iso2709Serializer.serialize(record));
                System.out.println(record);
                String hylla = MarcJSONConverter.toJSONString(record);
                System.out.println(hylla);
            }

        } catch (MalformedURLException mue) {
            mue.printStackTrace();
        }
    }

}
