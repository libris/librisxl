package whelk.importer;

/* lxl_import, Synopsis

0. Setup configuration variables for channel
1. Choose reader for channel (xml, iso2709, json, json-ld,tsv)
2. Transform (if necessary, possibly to several records)
3. For each record in (transformed) stream:
4. Select whelk-id's (for duplicate check)
5. Depending on id's (what if multiple?) and recordtype (bib,hold,auth)
6. Action=delete,merge,add,replace (from marc-leader? or config?)

*/

public class Import
{
	public static void main(String[] args)
	{
	}
}
