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
    public void testAppearanceOfSets() throws Exception
    {
        String response = TestCommon.httpGet("/oaipmh/?verb=ListSets");

        Assert.assertTrue(response.contains("auth"));
        Assert.assertTrue(response.contains("bib"));
        Assert.assertTrue(response.contains("hold"));
        Assert.assertTrue(response.contains("hold:S"));
        Assert.assertTrue(response.contains("hold:KVIN"));
        Assert.assertTrue(response.contains("hold:Gbg"));
    }

    @Test
    public void testExpandedFormAddsData() throws Exception
    {
        String records = TestCommon.httpGet("/oaipmh/?verb=ListRecords&metadataPrefix=jsonld&set=hold:S");
        String expandedRecords = TestCommon.httpGet("/oaipmh/?verb=ListRecords&metadataPrefix=jsonld_expanded&set=hold:S");

        Assert.assertTrue(expandedRecords.length() > (records.length() + "_expanded".length()));
    }

    /* Temporarily disabled due to XML schema BS.
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
                // Normal calls
                "/oaipmh/?verb=ListRecords&metadataPrefix=oai_dc&set=hold:S",
                "/oaipmh/?verb=ListIdentifiers&metadataPrefix=jsonld&set=hold:KVIN",
                "/oaipmh/?verb=ListIdentifiers&metadataPrefix=jsonld_expanded&set=hold:KVIN",
                "/oaipmh/?verb=ListIdentifiers&metadataPrefix=jsonld_includehold_expanded&set=hold:KVIN",
                "/oaipmh/?verb=Identify",
                "/oaipmh/?verb=ListSets",
                "/oaipmh/?verb=ListaMetadataFormats",
                "/oaipmh/?verb=GetRecord&metadataPrefix=oai_dc&identifier=https://libris.kb.se/7cjmx02d3hj2p81",

                // Error calls
                "/oaipmh/?verb=ListRecords",
                "/oaipmh/?verb=ListRecords&resumptionToken=1234",
                "/oaipmh/?verb=GetRecord&metadataPrefix=oai_dc&identifier=no_such_id",
                "/oaipmh/?verb=ListIdentifiers&metadataPrefix=jsonld&set=NO_SUCH_SET",
                "/oaipmh/?verb=ListIdentifiers&metadataPrefix=jsonld&until=1970-01-01",
                "/oaipmh/?verb=ListRecords&no_such_parameter=value",
        };

        for (String call : oaiPmhCalls)
        {
            String response = TestCommon.httpGet(call);
            validator.validate(new StreamSource(new StringReader(response)));
        }
    }*/

    @Test
    public void testNotAllowingUnknownParameters() throws Exception
    {
        final String oaiPmhCalls[] = {
                "/oaipmh/?verb=ListRecords&metadataPrefix=oai_dc&BAD_ARG=SOMETHING",
                "/oaipmh/?verb=ListIdentifiers&metadataPrefix=oai_dc&BAD_ARG=SOMETHING",
                "/oaipmh/?verb=Identify&BAD_ARG=SOMETHING",
                "/oaipmh/?verb=ListSets&BAD_ARG=SOMETHING",
                "/oaipmh/?verb=ListMetadataFormats&BAD_ARG=SOMETHING",
                "/oaipmh/?verb=GetRecord&BAD_ARG=SOMETHING",
        };

        for (String call : oaiPmhCalls)
        {
            String response = TestCommon.httpGet(call);
            Assert.assertTrue( response.contains("badArgument") );
        }
    }

    @Test
    public void testListRecordsBadResumptionToken() throws Exception
    {
        String response = TestCommon.httpGet("/oaipmh/?verb=ListRecords&metadataPrefix=oai_dc&resumptionToken=1234");
        Assert.assertTrue( response.contains("badResumptionToken") );
    }

    @Test
    public void testMetadataPrefixRequired() throws Exception
    {
        // Calls missing required &metaDataPrefix=..
        final String oaiPmhCalls[] = {
                "/oaipmh/?verb=ListRecords",
                "/oaipmh/?verb=GetRecord&identifier=dosent_matter"
        };

        for (String call : oaiPmhCalls)
        {
            String response = TestCommon.httpGet(call);
            Assert.assertTrue( response.contains("badArgument") );
        }
    }

    @Test
    public void testNoRecordsMatchOnEmptyLists() throws Exception
    {
        final String oaiPmhCalls[] = {
                "/oaipmh/?verb=ListRecords&metadataPrefix=oai_dc&until=1970-01-01",
                "/oaipmh/?verb=ListRecords&metadataPrefix=oai_dc_expanded&until=1970-01-01",
                "/oaipmh/?verb=ListIdentifiers&metadataPrefix=oai_dc&until=1970-01-01",
        };

        for (String call : oaiPmhCalls)
        {
            String response = TestCommon.httpGet(call);
            Assert.assertTrue( response.contains("noRecordsMatch") );
        }
    }

    @Test
    public void testHoldingsForBibSingle() throws Exception
    {
        String response = TestCommon.httpGet("/oaipmh/?verb=GetRecord&metadataPrefix=oai_dc_includehold&identifier=" +
                OaiPmh.configuration.getProperty("baseUri") + "l4xz4wvx42rfsnb");
        Assert.assertTrue( response.contains("holding sigel=\"Gbg\" id=\"59hm0xrb4wxl4bm\"") );
    }

    @Test
    public void testHoldingsForBibMultiple() throws Exception
    {
        String response = TestCommon.httpGet("/oaipmh/?verb=ListRecords&metadataPrefix=oai_dc_includehold");
        Assert.assertTrue( response.contains("holding sigel=\"Gbg\" id=\"59hm0xrb4wxl4bm\"") );
    }

    @Test
    public void testHoldSigelSets() throws Exception
    {
        String response = TestCommon.httpGet("/oaipmh/?verb=ListIdentifiers&set=hold:S&metadataPrefix=oai_dc");
        Assert.assertFalse( response.contains("noRecordsMatch"));
    }

    @Test
    public void testBibSigelSets() throws Exception
    {
        String response = TestCommon.httpGet("/oaipmh/?verb=ListIdentifiers&set=bib:S&metadataPrefix=oai_dc");
        Assert.assertFalse( response.contains("noRecordsMatch"));
    }

    /* This feature had to be disabled for performance reasons.
    @Test
    public void testBibSigelSetsInHeaderOnGetRecord() throws Exception
    {
        String response = TestCommon.httpGet("/oaipmh/?verb=GetRecord&metadataPrefix=oai_dc&identifier=" +
                OaiPmh.configuration.getProperty("baseUri") + "fxql7jqr38b1dkf");
        Assert.assertTrue( response.contains("bib:Gbg"));
    }

    @Test
    public void testBibSigelSetsInHeaderOnListRecords() throws Exception
    {
        String response = TestCommon.httpGet("/oaipmh/?verb=ListRecords&set=bib:S&metadataPrefix=oai_dc");
        Assert.assertTrue( response.contains("bib:Gbg"));
        Assert.assertTrue( response.contains("bib:S"));
    }*/

    @Test
    public void testAuthSet() throws Exception
    {
        String response = TestCommon.httpGet("/oaipmh/?verb=ListRecords&set=auth&metadataPrefix=oai_dc");
        Assert.assertTrue( !response.contains("noRecordsMatch") );
    }

    @Test
    public void testBibSet() throws Exception
    {
        String response = TestCommon.httpGet("/oaipmh/?verb=ListRecords&set=bib&metadataPrefix=oai_dc");
        Assert.assertTrue( !response.contains("noRecordsMatch") );
    }

    @Test
    public void testHoldSet() throws Exception
    {
        String response = TestCommon.httpGet("/oaipmh/?verb=ListRecords&set=hold&metadataPrefix=oai_dc");
        Assert.assertTrue( !response.contains("noRecordsMatch") );
    }

    @Test
    public void testHoldAboutBib() throws Exception
    {
        String response = TestCommon.httpGet("/oaipmh/?verb=ListRecords&metadataPrefix=jsonld&set=hold");
        Assert.assertTrue( response.contains("itemOf id=\"http") );
    }
}
