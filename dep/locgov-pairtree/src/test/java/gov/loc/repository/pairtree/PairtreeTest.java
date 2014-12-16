package gov.loc.repository.pairtree;

import gov.loc.repository.pairtree.Pairtree.InvalidPpathException;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class PairtreeTest {

	Pairtree pt = new Pairtree();
	
	@Before
	public void setup() {
		pt.setSeparator('/');
	}
	
	@Test
	public void testMapToPPath() {
		assertEquals("ab/cd", pt.mapToPPath("abcd"));
		assertEquals("ab/cd/ef/g", pt.mapToPPath("abcdefg"));
		assertEquals("12/-9/86/xy/4", pt.mapToPPath("12-986xy4"));
		
		assertEquals("13/03/0_/45/xq/v_/79/38/42/49/5", pt.mapToPPath(null, "13030_45xqv_793842495", null));
		assertEquals("13/03/0_/45/xq/v_/79/38/42/49/5/793842495", pt.mapToPPath(null, "13030_45xqv_793842495", "793842495"));
		assertEquals("/data/13/03/0_/45/xq/v_/79/38/42/49/5", pt.mapToPPath("/data", "13030_45xqv_793842495", null));
		assertEquals("/data/13/03/0_/45/xq/v_/79/38/42/49/5", pt.mapToPPath("/data/", "13030_45xqv_793842495", null));
		assertEquals("/data/13/03/0_/45/xq/v_/79/38/42/49/5/793842495", pt.mapToPPath("/data", "13030_45xqv_793842495", "793842495"));
		
	}
	
	@Test
	public void testIdCleaning() {
		assertEquals("ark+=13030=xt12t3", pt.cleanId("ark:/13030/xt12t3"));
		assertEquals("http+==n2t,info=urn+nbn+se+kb+repos-1", pt.cleanId("http://n2t.info/urn:nbn:se:kb:repos-1"));
		assertEquals("what-the-^2a@^3f#!^5e!^3f", pt.cleanId("what-the-*@?#!^!?"));
	}

	@Test
	public void testIdUncleaning() {
		assertEquals("ark:/13030/xt12t3", pt.uncleanId("ark+=13030=xt12t3"));
		assertEquals("http://n2t.info/urn:nbn:se:kb:repos-1", pt.uncleanId("http+==n2t,info=urn+nbn+se+kb+repos-1"));
		assertEquals("what-the-*@?#!^!?", pt.uncleanId("what-the-^2a@^3f#!^5e!^3f"));
	}
	
	@Test
	public void testMapToPPathWithIdCleaning() {
		assertEquals("ar/k+/=1/30/30/=x/t1/2t/3", pt.mapToPPath("ark:/13030/xt12t3"));
		
		assertEquals("ht/tp/+=/=n/2t/,i/nf/o=/ur/n+/nb/n+/se/+k/b+/re/po/s-/1", pt.mapToPPath("http://n2t.info/urn:nbn:se:kb:repos-1"));
		assertEquals("wh/at/-t/he/-^/2a/@^/3f/#!/^5/e!/^3/f", pt.mapToPPath("what-the-*@?#!^!?"));
	}
	
	@Test
	public void testExtractEncapsulatingDir() throws InvalidPpathException {
		assertNull(pt.extractEncapsulatingDirFromPpath("ab"));
		assertNull(pt.extractEncapsulatingDirFromPpath("ab/cd"));
		assertNull(pt.extractEncapsulatingDirFromPpath("ab/cd/"));
		assertNull(pt.extractEncapsulatingDirFromPpath("ab/cd/ef/g"));
		assertNull(pt.extractEncapsulatingDirFromPpath("ab/cd/ef/g/"));
		assertEquals("h", pt.extractEncapsulatingDirFromPpath("ab/cd/ef/g/h"));
		assertEquals("h", pt.extractEncapsulatingDirFromPpath("ab/cd/ef/g/h/"));
		assertEquals("efg", pt.extractEncapsulatingDirFromPpath("ab/cd/efg"));
		assertEquals("efg", pt.extractEncapsulatingDirFromPpath("ab/cd/efg/"));
		assertEquals("h", pt.extractEncapsulatingDirFromPpath("ab/cd/ef/g/h"));
		assertEquals("h", pt.extractEncapsulatingDirFromPpath("ab/cd/ef/g/h/"));

		assertNull(pt.extractEncapsulatingDirFromPpath("/data", "/data/ab"));
		assertNull(pt.extractEncapsulatingDirFromPpath("/data/", "/data/ab"));
		assertEquals("h", pt.extractEncapsulatingDirFromPpath("/data", "/data/ab/cd/ef/g/h"));
		assertEquals("h", pt.extractEncapsulatingDirFromPpath("/data/", "/data/ab/cd/ef/g/h"));

	}

	@Test
	public void testMapToId() throws InvalidPpathException {
		assertEquals("ab", pt.mapToId("ab"));
		assertEquals("abcd", pt.mapToId("ab/cd"));
		assertEquals("abcd", pt.mapToId("ab/cd/"));
		assertEquals("abcdefg", pt.mapToId("ab/cd/ef/g"));
		assertEquals("abcdefg", pt.mapToId("ab/cd/ef/g/"));
		assertEquals("abcdefg", pt.mapToId("ab/cd/ef/g/h"));
		assertEquals("abcdefg", pt.mapToId("ab/cd/ef/g/h/"));
		assertEquals("abcd", pt.mapToId("ab/cd/efg"));
		assertEquals("abcd", pt.mapToId("ab/cd/efg/"));
		assertEquals("abcdefg", pt.mapToId("ab/cd/ef/g/h"));
		assertEquals("abcdefg", pt.mapToId("ab/cd/ef/g/h/"));

		assertEquals("ab/cd/ef/g", pt.mapToPPath("abcdefg"));
		assertEquals("12-986xy4", pt.mapToId("12/-9/86/xy/4"));

		assertEquals("13030_45xqv_793842495", pt.mapToId("13/03/0_/45/xq/v_/79/38/42/49/5"));
		assertEquals("13030_45xqv_793842495", pt.mapToId("13/03/0_/45/xq/v_/79/38/42/49/5/793842495"));
		assertEquals("13030_45xqv_793842495", pt.mapToId("/data", "/data/13/03/0_/45/xq/v_/79/38/42/49/5"));
		assertEquals("13030_45xqv_793842495", pt.mapToId("/data/", "/data/13/03/0_/45/xq/v_/79/38/42/49/5"));
		assertEquals("13030_45xqv_793842495", pt.mapToId("/data", "/data/13/03/0_/45/xq/v_/79/38/42/49/5/793842495"));		
	}

	
	@Test(expected=Pairtree.InvalidPpathException.class)
	public void testInvalidExtractEncapsulatingDir1() throws InvalidPpathException {
		pt.extractEncapsulatingDirFromPpath("abc");
	}

	@Test(expected=Pairtree.InvalidPpathException.class)
	public void testInvalidExtractEncapsulatingDir2() throws InvalidPpathException {
		pt.extractEncapsulatingDirFromPpath("ab/cdx/efg/");
	}

	@Test
	public void testMapToIdWithIdCleaning() throws InvalidPpathException {
		assertEquals("ark:/13030/xt12t3", pt.mapToId("ar/k+/=1/30/30/=x/t1/2t/3"));
		
		//This example from the spec is wrong
		//assertEquals("http://n2t.info/urn:nbn:se:kb:repos-1", pt.mapToId("ht/tp/+=/=n/2t/,i/nf/o=/ur/n+/n/bn/+s/e+/kb/+/re/p/os/-1"));
		assertEquals("http://n2t.info/urn:nbn:se:kb:repos-1", pt.mapToId("ht/tp/+=/=n/2t/,i/nf/o=/ur/n+/nb/n+/se/+k/b+/re/po/s-/1"));
		assertEquals("what-the-*@?#!^!?", pt.mapToId("wh/at/-t/he/-^/2a/@^/3f/#!/^5/e!/^3/f"));
	}
	
}
