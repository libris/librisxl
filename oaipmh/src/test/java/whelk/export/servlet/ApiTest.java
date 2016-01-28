package whelk.export.servlet;

import org.junit.Assert;
import org.junit.*;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;

import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.File;
import java.io.StringReader;

public class ApiTest
{
    /**
     * These tests are based on the assumption that the test data (see /librisxl-tools/scripts/example_records.tsv) is
     * loaded into a database, and the secrets.properties file correctly addresses this database.
     */

    private static Server s_jettyServer;

    @BeforeClass
    public static void setUp()
            throws Exception
    {
        // Start the OAI-PMH servlet in an embedded jetty container
        s_jettyServer = new Server(TestCommon.port);
        ServletHandler servletHandler = new ServletHandler();
        s_jettyServer.setHandler(servletHandler);
        servletHandler.addServletWithMapping(OaiPmh.class, "/*");
        s_jettyServer.start();
    }

    @AfterClass
    public static void tearDown()
            throws Exception
    {
        s_jettyServer.stop();
    }

    @Test
    public void testIdentify() throws Exception
    {
        String result = TestCommon.httpGet("/oaipmh/?verb=Identify");

        if ( ! result.contains("Libris XL") )
            Assert.fail("Identify did not contain 'Libris XL'.");
    }

    @Test
    public void testIdentifyAndGetRecord() throws Exception
    {
        String identifiers = TestCommon.httpGet("/oaipmh/?verb=ListIdentifiers&metadataPrefix=jsonld&set=hold:S");
        String identifier = TestCommon.extractFirstOccurrenceElementContents(identifiers, "identifier");
        String record = TestCommon.httpGet("/oaipmh/?verb=GetRecord&metadataPrefix=jsonld&identifier=" + identifier);

        if (record.contains("noRecordsMatch"))
            Assert.fail("No record for provided identifier.");
    }

    @Test
    public void testAppearanceOfAuthRecords() throws Exception
    {
        String records = TestCommon.httpGet("/oaipmh/?verb=ListRecords&metadataPrefix=jsonld&set=auth");

        Assert.assertTrue(records.contains("Johannes de Hesse"));
        Assert.assertTrue(records.contains("Trippelkonserten"));
        Assert.assertTrue(records.contains("Trippelkonserten"));
    }

    @Test
    public void testAppearanceOfBibRecords() throws Exception
    {
        String records = TestCommon.httpGet("/oaipmh/?verb=ListRecords&metadataPrefix=jsonld&set=bib");

        Assert.assertTrue(records.contains("Öfwer-Ståthållare-Embetets Kungörelse, at Krögare och Närings-Idkare"));
        Assert.assertTrue(records.contains("Stålmannens nya jätte-tidning"));
        Assert.assertTrue(records.contains("Blade runner"));
    }

    @Test
    public void testExpandedFormAddsData() throws Exception
    {
        String records = TestCommon.httpGet("/oaipmh/?verb=ListRecords&metadataPrefix=jsonld&set=hold:S");
        String expandedRecords = TestCommon.httpGet("/oaipmh/?verb=ListRecords&metadataPrefix=jsonld:expanded&set=hold:S");

        final String inExpandedOnly[] = {
                "Sveriges ledande magasin om kultur och samhälle",
                "Anteckningar från en ö",
                "Stålmannens jätte-tidning"
        };

        for (String s : inExpandedOnly)
        {
            Assert.assertTrue( ! records.contains(s) );
            Assert.assertTrue( expandedRecords.contains(s) );
        }
    }

    @Test
    public void testOaiPmhSchemaValidation() throws Exception
    {
        SchemaFactory fact = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        Source[] sources =
                {
                        new StreamSource(new File( getClass().getClassLoader().getResource("OAI-PMH.xsd").getFile() )),
                        new StreamSource(new File( getClass().getClassLoader().getResource("oai_dc.xsd").getFile() )),
                };
        Schema schema = fact.newSchema(sources);
        Validator validator = schema.newValidator();

        final String oaiPmhCalls[] = {
                "/oaipmh/?verb=ListRecords&metadataPrefix=oai_dc&set=hold:S",
                "/oaipmh/?verb=ListIdentifiers&metadataPrefix=jsonld&set=hold:KVIN",
                "/oaipmh/?verb=Identify",
                "/oaipmh/?verb=ListSets",
                "/oaipmh/?verb=ListaMetadataFormats",
                // The GetRecord case is commented out, because identifiers are scrambled on re-reading the testdata.
                //"/oaipmh/?verb=GetRecord&metadataPrefix=oai_dc&identifier=https://libris.kb.se/7cjmx02d3hj2p81"
        };

        for (String call : oaiPmhCalls)
        {
            String response = TestCommon.httpGet(call);
            validator.validate(new StreamSource(new StringReader(response)));
        }
    }
}
