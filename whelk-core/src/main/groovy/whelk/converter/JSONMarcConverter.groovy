package whelk.converter

import javax.xml.transform.*
import javax.xml.transform.dom.*
import javax.xml.transform.stream.*
import org.w3c.dom.*

import se.kb.libris.util.marc.Datafield
import se.kb.libris.util.marc.impl.ControlfieldImpl
import se.kb.libris.util.marc.impl.DatafieldImpl
import se.kb.libris.util.marc.impl.MarcRecordImpl
import se.kb.libris.util.marc.io.DomSerializer

import groovy.util.logging.Log4j2 as Log

import se.kb.libris.util.marc.Controlfield
import se.kb.libris.util.marc.MarcRecord
import org.codehaus.jackson.map.ObjectMapper


@Log
class JSONMarcConverter {
    protected final static ObjectMapper mapper = new ObjectMapper()

    static MarcRecord fromJson(String marcJson) {
        Map resultJson = mapper.readValue(marcJson, Map)

        return fromJsonMap(resultJson)
    }


    static MarcRecord fromJsonMap(Map marcJson) {

        MarcRecord record = new MarcRecordImpl()
        def fields = marcJson.get("fields")
        def leader = marcJson.get("leader")

        record.setLeader(leader)

        for (Map field in fields) {
            field.each {String fieldKey, fieldValue ->

                if (fieldKey.isInteger() && fieldKey.toInteger() < 10){
                    Controlfield controlfield = new ControlfieldImpl(fieldKey, fieldValue)
                    record.addField(controlfield)
                }else {
                    Datafield datafield = new DatafieldImpl(fieldKey)
                    if (fieldValue instanceof Map) {
                        fieldValue.each {dataKey, dataValue ->
                            if (dataValue instanceof ArrayList) {
                                for (Map subFields in dataValue) {
                                    subFields.each {subKey, subValue ->
                                       datafield.addSubfield(subKey as char, subValue)
                                    }
                                }
                            }else if(dataValue instanceof Map) {
                                dataValue.each {subKey, subValue ->
                                    datafield.addSubfield(subKey as char, subValue)
                                }
                            }else {
                                int ind = 1
                                if (dataKey.equals("ind1"))
                                    ind = 0
                                if (dataValue.equals(""))
                                    dataValue = " "
                                datafield.setIndicator(ind, dataValue as char)
                            }
                        }
                        record.addField(datafield)
                    }
                }
            }
        }
        return record
    }

    static String marcRecordAsXMLString(MarcRecord record) {
        DocumentFragment docFragment = marcRecordAsXMLFragment(record)
        StringWriter sw = new StringWriter()
        Source source = new DOMSource(docFragment)
        Result result = new StreamResult(sw)
        Transformer transformer = javax.xml.transform.TransformerFactory.newInstance().newTransformer()
        transformer.setOutputProperty("omit-xml-declaration", "yes")

        try {
            transformer.setOutputProperty("encoding", "UTF-8")
            transformer.transform(source, result)
        } catch (javax.xml.transform.TransformerException e) {
            System.err.println(e.getMessage())
        }

        return sw.toString()
    }

    static DocumentFragment marcRecordAsXMLFragment(MarcRecord record) {
        DocumentFragment docFragment = DomSerializer.serialize(record, javax.xml.parsers.DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument())
        return docFragment
    }
}
