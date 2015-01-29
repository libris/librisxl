package whelk.api

import groovy.util.logging.Slf4j as Log
import groovy.xml.StreamingMarkupBuilder
import whelk.Document
import whelk.converter.libris.JsonLD2MarcXMLConverter

import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import java.text.DateFormat
import java.text.ParseException
import java.text.SimpleDateFormat

@Log
class OaipmhSearchAPI extends BasicAPI {

    static final String DATE_TIME_PATTERN = "yyyy-MM-dd'T'HH:MM:ss'Z'"
    JsonLD2MarcXMLConverter jsonLD2MarcXMLConverter = null
    def result              = new StringWriter()
    def builder             = new StreamingMarkupBuilder()
    def defualtNamespace    = 'http://www.openarchives.org/OAI/2.0/'
    def xsiNamespace        = 'http://www.w3.org/2001/XMLSchema-instance'
    def schemaLocation      = 'http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd'
    def cache               = new HashMap()
    Iterable<Document> allDocuments = null
    String token
    static NR_OF_ROWS = 50
    def resumptionTokens = new HashMap()
    def validDataSets = ["bib", "auth", "hold"]

    @Override
    String getDescription() {
        return null
    }
    // auth, bib, ?
    @Override
    protected void doHandle(HttpServletRequest request, HttpServletResponse response, List pathVars) {

        if (jsonLD2MarcXMLConverter == null) {
            jsonLD2MarcXMLConverter = plugins.find { it instanceof JsonLD2MarcXMLConverter }
        }

        def pathInfo = request.pathInfo.substring(1).split("/")
        def verb            = request.getParameter("verb")
        def metadataPrefix  = request.getParameter("metadataPrefix")//TODO
        def resumptionToken = request.getParameter("resumptionToken")
        def since           = request.getParameter("from")
        def body
        def error = false

        if (pathInfo == 0 || !validDataSets.contains(pathInfo[0])) {
            body = buildErrorMessages("FEL!!!")
            error = true
        }

        resumptionTokens.each {resumptionToken
            def now = System.currentTimeMillis()
            def expires = resumptionTokens.get(resumptionToken)
            if (expires > now) {
                resumptionTokens.remove(resumptionToken)
                cache.remove(resumptionToken)
            }
        }
        if (!isNullOrEmpty(resumptionToken))
            resumptionTokens.put(resumptionToken, System.currentTimeMillis())

        if (isNullOrEmpty(metadataPrefix) || request.getParameterValues("metadataPrefix").size() > 1) {
            body = buildErrorMessages("cannotDisseminateFormat")
            error = true
        }

        if (!"ListRecords".equals(verb) || request.getParameterValues("verb").size() > 1) {
            body = buildErrorMessages("badVerb"  )
            error = true
        }else {

            try {
                DateFormat df = new SimpleDateFormat("yyyy-MM-ddhh:MM:ss")
                Date sinceDate = since != null ? df.parse(since) : null
                allDocuments = whelk.loadAll(pathInfo[0], sinceDate) //TODO
            }catch (ParseException e) {
                body = buildErrorMessages("noRecordsMatch", "Wrong date format")
                error = true
            }

            if (allDocuments == null || allDocuments.iterator().size() == 0) {
                body = buildErrorMessages("noRecordsMatch")
                error = true
            }

            List records = new ArrayList()
            if (cache.containsKey(resumptionToken)) {

                if (cache.get(resumptionToken) instanceof ArrayList) {
                    records.addAll(cache.get(resumptionToken))
                }else {

                    List resultList = new ArrayList()
                    def documents = cache.get(resumptionToken)
                    int count = 0
                    for (Iterator<Document> it = documents.iterator(); it.hasNext();) {
                        count++
                        Document doc = jsonLD2MarcXMLConverter.doConvert(it.next())
                        records.add(doc)
                        resultList.add(doc) //<---
                        if (count == NR_OF_ROWS) {
                            token = "${new Date(doc.modified).format(DATE_TIME_PATTERN)}|${doc.identifier.substring(doc.identifier.lastIndexOf("/")+1)}|origin:NB|${metadataPrefix}"
                            cache.put(token, it)
                            break
                        }
                    }
                    cache.putAt(resumptionToken, resultList)
                }
            }else{
                int count = 0
                for (Iterator<Document> it = allDocuments.iterator(); it.hasNext();) {
                    count++
                    Document doc = jsonLD2MarcXMLConverter.doConvert(it.next())
                    records.add(doc)
                    if (count == NR_OF_ROWS) {
                        token = "${new Date(doc.modified).format(DATE_TIME_PATTERN)}|${doc.identifier.substring(doc.identifier.lastIndexOf("/") + 1)}|origin:NB|${metadataPrefix}"
                        cache.put(token, it)
                        break;
                    }
                }
            }
            if (!error)
                body = buildMarcXml(records, verb, metadataPrefix)
        }
        sendResponse(response, body, "application/xml")

    }

   private String buildMarcXml(ArrayList documents, def verb = null, metadataPrefix = null) {
        def root =  {
            mkp.xmlDeclaration()
            mkp.declareNamespace('':defualtNamespace)
            mkp.declareNamespace('xsi':xsiNamespace)
            "OAI-PMH"('xsi:schemaLocation':schemaLocation) {
                responseDate(new Date().format(DATE_TIME_PATTERN, TimeZone.getTimeZone('UTC')))
                request("verb":verb, "metadataPrefix":metadataPrefix)
                ListRecords() {
                    documents.each {
                        def doc = it
                        record() {
                            header() {
                                identifier("http://libris.kb.se/resource"+doc.identifier)
                                datestamp(new Date(doc.modified).format(DATE_TIME_PATTERN))
                                //setSpec("cs") //TODO
                            }
                            metadata() {
                                mkp.declareNamespace('marc':'http://www.loc.gov/MARC21/slim')
                                mkp.declareNamespace('xsi':'http://www.w3.org/2001/XMLSchema-instance')
                                mkp.yieldUnescaped(doc.dataAsString)
                            }
                        }
                    }
                }
                if (token != null) {
                    resumptionToken(token)
                    token = null
                }
            }
        }
        return  builder.bind(root).toString()
    }

    private String buildErrorMessages(def errorValue, def message = null) {
        def root = {
            mkp.xmlDeclaration()
            mkp.declareNamespace('': defualtNamespace)
            mkp.declareNamespace('xsi': xsiNamespace)
            "OAI-PMH"('xsi:schemaLocation': schemaLocation) {
                responseDate(new Date().format(DATE_TIME_PATTERN, TimeZone.getTimeZone('UTC')))
                error('code': errorValue, message)
            }
        }

        return builder.bind(root).toString()
    }

    private boolean isNullOrEmpty(String s) {
        if (s == null || s.isEmpty())
            return true
        return false
    }
}