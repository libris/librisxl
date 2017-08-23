# lxl_import

Libris XL batch import of metadata.


## Prerequisities

* Java (8+) for the core importer program
* Gradle for building
* Perl (version ??) for certain data fetching scripts


## Usage

The importing mechanism for Libris XL consists of a number of scripts (see the scripts directory) that are used for fetching data from various sources and placing it as files in a directory structure. These files are then used as the input to the lxl_import java program which transforms and imports the data into the Whelk (Libris XL).

In order to function, this program needs a Libris XL secret properties file. Please consult the [whelk-core](https://github.com/libris/whelk-core) readme, for information on how to build such a file

lxl_import expects records fed to it to have a certain order, such that each bibliograhic record be followed by the holding records for that record (if any). This ordering of the data must be in effect at the latest after any XSLT transforms have been applied. This program should NOT be used to import authority records.

Parameters:

### --path
A file path to use as input data. If this path represents a folder, all files in that folder will be imported (but no subfolders). If the path represents a file, only that specific file will be imported. If this parameter is omitted, lxl_import will expect its input on stdin.

### --format
The format of the input data. Can be either "iso2709" or "xml". This parameter must be specified. If the format is "xml", the structure of the xml document must be that of MARCXML (as defined by [jmarctools](https://github.com/libris/jmarctools), nominally: www.loc.gov/MARC21/slim) at the latest after any XSLT transforms have been applied.

### --transformer
The path to an XSLT stylsheet that should be used to transform the input before importing. This parameter may be used even if the input format is "iso2709", in which case the stream will be translated to MARCXML before transformation. If more than one transformer is specified these will be applied in the same order they are specified on the command line. XSLT transformation is optional.

### --inEncoding
The character encoding of the incoming data. Only relevant if the format is "iso2709" as xml documents are expected to declare their encoding in the xml header. Defaults to UTF-8.

### --dupType
The type of duplication checking that should be done for each incoming record. The value of this parameter may be a comma-separated list of any combination of duplication types. If a duplicate is found for an incoming record, that record will be enriched with any additional information in the incoming record.

### --live
Write to Whelk (without this flag operations against the Whelk are readonly, and results are only printed to stdout).

## Duplication types

- ISBNA     ISBN number, obtained from MARC subfield $a of the incoming record
- ISBNZ     ISBN number, obtained from MARC subfield $z of the incoming record
- ISSNA     ISSN number, obtained from MARC subfield $a of the incoming record
- ISSNZ     ISSN number, obtained from MARC subfield $z of the incoming record
- 035A      ID in other system, obtained from MARC 035 $a of the incoming record
- LIBRIS-ID ID in Libris.

## Example usage
    $ java -Dxl.secret.properties=./secret.properties -jar build/libs/lxl_import.jar --format=xml --path=input/adlibrismessedup/ --transformer=transformers/forvarv.xsl --dupType=ISSNA --live