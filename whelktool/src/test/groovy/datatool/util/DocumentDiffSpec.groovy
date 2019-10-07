package datatool.util;

import org.codehaus.jackson.map.ObjectMapper

import spock.lang.Shared
import spock.lang.Specification

class DocumentDiffSpec extends Specification {
	@Shared
	ObjectMapper mapper = new ObjectMapper()
	
	def "document should be same as itself"() {
		given:
		Map a = loadTestData('work1.json')
		
		expect:
		new DocumentDiff().equals(a, a) == true
//		new DocumentDiff().subset(a, b) == true
//		new DocumentDiff().subset(b, a) == true
	}
	
	def "order of elements should not matter"() {
		given:
		Map a = loadTestData('work1.json')
		Map b = loadTestData('work1-reordered.json')
		
		expect:
		new DocumentDiff().equals(a, b) == true
//		new DocumentDiff().subset(a, b) == true
//		new DocumentDiff().subset(b, a) == true
	}
	
	def "subset"() {
		given:
		Map a = loadTestData('work1.json')
		Map b = loadTestData('work1-subset.json')
		DocumentDiff diff = new DocumentDiff() 
		
		expect:
//		diff.equals(a, b) == false
		diff.subset(a, b) == false
//		diff.subset(b, a) == true
	}
	
	def "not equal"() {
		given:
		Map a = loadTestData('work1.json')
		Map b = loadTestData('work1-changed.json')
		DocumentDiff diff = new DocumentDiff()
		
		expect:
		diff.equals(a, b) == false
//		diff.subset(a, b) == false
//		diff.subset(b, a) == false
	}
	
	def "order matters for termComponentList"() {
		given:
		Map a = loadTestData('ordered.json')
		Map b = loadTestData('ordered-reordered.json')
		DocumentDiff diff = new DocumentDiff()
		
		expect:
		diff.equals(a, b) == false
//		diff.subset(a, b) == false
//		diff.subset(b, a) == false
	}
	
	private Map loadTestData(String file) {
		String json = this.getClass().getResource(file).text
		return mapper.readValue(json, Map.class)
	}
}
