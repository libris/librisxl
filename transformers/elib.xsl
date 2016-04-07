<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0" xmlns:java="http://xml.apache.org/xslt/java" exclude-result-prefixes="java">
    <xsl:output method="xml" indent="yes" encoding="UTF-8"/>
    
    <!-- transformation date -->
    <xsl:variable name="rawdatestamp"><xsl:value-of select="java:format(java:java.text.SimpleDateFormat.new('yyyy-MM-dd'), java:java.util.Date.new())"/></xsl:variable>
    <xsl:variable name="datestamp"><xsl:value-of select="substring(translate($rawdatestamp, '-', ''), 3, 6)"/></xsl:variable>
    
    <xsl:template match="data">
        <collection xmlns="http://www.loc.gov/MARC21/slim">
            <xsl:apply-templates select="product"/>
        </collection>
    </xsl:template>
    
    <xsl:template match="product">
        <xsl:if test="translate(normalize-space(status/description), 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö') = 'aktiv'">
            <xsl:variable name="isbn13">
                <xsl:apply-templates select="external_ids/external_id/id_type[normalize-space(.) = 'ISBN13']"/>
            </xsl:variable>
            <xsl:variable name="isbn10">
                <xsl:apply-templates select="external_ids/external_id/id_type[normalize-space(.) = 'ISBN']"/>
            </xsl:variable>
            <xsl:variable name="format_group">
                <xsl:choose>
                    <xsl:when test="formats/format[1]/format_id = '50' or formats/format[1]/format_id = '54' or formats/format[1]/format_id = '56' or formats/format[1]/format_id = '58'">e-book</xsl:when>
                    <xsl:when test="formats/format[1]/format_id = '71' or formats/format[1]/format_id = '75' or formats/format[1]/format_id = '230'">audiobook</xsl:when>
                </xsl:choose>
            </xsl:variable> 
            
            <!-- Bibliographic record -->
            <record type="Bibliographic">
                <xsl:apply-templates select="formats/format[1]" mode="bibliographic"><xsl:with-param name="mediatype" select="$format_group"/></xsl:apply-templates>
                <xsl:apply-templates select="updated_date"/>
                <xsl:apply-templates select="title"/>
                <xsl:apply-templates select="status/id" mode="bibliographic"/>
                <!--<xsl:apply-templates select="coverimage"/>-->                
                <xsl:call-template name="external_id">
                    <xsl:with-param name="isbn">
                        <xsl:choose>
                            <xsl:when test="$isbn13 != ''"><xsl:value-of select="$isbn13"/></xsl:when>
                            <xsl:when test="$isbn10 != ''"><xsl:value-of select="$isbn10"/></xsl:when>
                        </xsl:choose>
                    </xsl:with-param>
                </xsl:call-template>
                <xsl:call-template name="generate_008"><xsl:with-param name="isbn" select="$isbn10"/><xsl:with-param name="isbn13" select="$isbn13"/><xsl:with-param name="media_type" select="$format_group"/></xsl:call-template>
                <xsl:apply-templates select="contributors/contributor"><xsl:with-param name="formatgroup" select="$format_group"/></xsl:apply-templates>
                <xsl:apply-templates select="publisher/name"/>
                <xsl:apply-templates select="description"/>
                <xsl:apply-templates select="categories"/>
                <!--<datafield ind1=" " ind2=" " tag="599">
                    <subfield code="a">Maskinellt genererad post. Ändra kod för fullständighetsnivå (leader/17), annars kommer manuellt gjorda ändringar att försvinna.</subfield>
                </datafield>-->
            </record>
            
            <!-- Holdings record -->
            <record type="Holdings">
                <xsl:variable name="libris_leader">
                    <xsl:choose>
                        <xsl:when test="normalize-space(libris_leader) != ''"><xsl:value-of select="normalize-space(libris_leader)"/></xsl:when>
                        <xsl:otherwise>n</xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
                <leader>*****<xsl:value-of select="$libris_leader"/>x  a22*****1n 4500</leader>
                <controlfield tag="008"><xsl:value-of select="$datestamp"/>||0000|||||000||||||000000</controlfield>
                <!--<xsl:apply-templates select="status/id" mode="holdings"/>-->
               <!-- <xsl:variable name="isbn13_tmp">
                    <xsl:apply-templates select="external_ids/external_id/id_type[normalize-space(.) = 'ISBN13']"/>
                </xsl:variable>
                <xsl:variable name="isbn10_tmp">
                    <xsl:apply-templates select="external_ids/external_id/id_type[normalize-space(.) = 'ISBN']"/>
                </xsl:variable>-->
     			<xsl:call-template name="generate_035">
                    <xsl:with-param name="isbn">
                        <xsl:choose>
                            <xsl:when test="$isbn13 != ''"><xsl:value-of select="$isbn13"/></xsl:when>
                            <xsl:when test="$isbn10 != ''"><xsl:value-of select="$isbn10"/></xsl:when>
                        </xsl:choose>
                    </xsl:with-param>
                </xsl:call-template>
                <xsl:apply-templates select="formats/format" mode="holdings"><xsl:with-param name="mediatype" select="$format_group"/></xsl:apply-templates>
                <xsl:if test="$isbn13 != ''"><xsl:call-template name="generate_856"><xsl:with-param name="isbn_tmp" select="$isbn13"/></xsl:call-template></xsl:if>
                <xsl:apply-templates select="teaser/link"><xsl:with-param name="mediatyp" select="$format_group"/></xsl:apply-templates>
           </record>
         </xsl:if>
    </xsl:template>
    
    <!-- format, leader, controlfield 007, datafield 655 -->
    <xsl:template match="formats/format" mode="bibliographic">
	    <xsl:param name="mediatype"/>
        <xsl:variable name="leader_06">
            <xsl:choose>
                <xsl:when test="$mediatype = 'e-book'">a</xsl:when>
                <xsl:when test="$mediatype = 'audiobook'">i</xsl:when>
                <xsl:otherwise> </xsl:otherwise>
            </xsl:choose>    
        </xsl:variable>
        <leader>*****n<xsl:value-of select="$leader_06"/>m a22*****3a 4500</leader>
        <xsl:if test="$mediatype = 'audiobook'"><controlfield tag="007">su||||||||||||</controlfield></xsl:if>
        <controlfield tag="007">cr||||||||||||</controlfield>
        <xsl:if test="$mediatype = 'e-book' or $mediatype = 'audiobook'">
            
                <xsl:choose>
                    <xsl:when test="$mediatype = 'e-book'">
                        <datafield ind1=" " ind2="4" tag="655">
                            <subfield code="a">E-böcker</subfield>
                        </datafield>
                    </xsl:when>
                    <xsl:when test="$mediatype = 'audiobook'">
                        <datafield ind1=" " ind2="7" tag="655">
                            <subfield code="a">Ljudböcker</subfield>
                            <subfield code="2">saogf</subfield>
                        </datafield>
                    </xsl:when>
                </xsl:choose>
        </xsl:if>
        <xsl:if test="comment != '' and $mediatype = 'audiobook'">
            <datafield ind1="" ind2="" tag="500">
                <subfield code="a"><xsl:value-of select="comment"/></subfield>
            </datafield>
        </xsl:if>
    </xsl:template>
    
    <!-- format, datafield 852 -->
    <xsl:template match="formats/format" mode="holdings">
        <xsl:param name="mediatype"/>
        <datafield ind1=" " ind2=" " tag="852">
            <subfield code="b">Elib</subfield>
            <xsl:choose>
                <xsl:when test="$mediatype = 'e-book'">
                    <subfield code="h">E-Bok</subfield>        
                </xsl:when>
                <xsl:when test="$mediatype = 'audiobook'">
                    <subfield code="h">Ljudbok</subfield>        
                </xsl:when>
            </xsl:choose>
            <xsl:if test="name != ''">
                <subfield code="z"><xsl:value-of select="normalize-space(name)"/></subfield>
            </xsl:if>
            <xsl:if test="size_bytes != ''">
                <xsl:variable name="mb_tmp" select="number(normalize-space(size_bytes)) div 1000000"/>
                <xsl:variable name="decimals_tmp" select="substring-after($mb_tmp, '.')"/>
                <xsl:variable name="no_dec_tmp" select="substring-before($mb_tmp, '.')"/>
                <xsl:variable name="last_rounded_dec_tmp">
                    <xsl:choose>
                        <xsl:when test="number(substring($decimals_tmp, 3, 1)) > 4"><xsl:value-of select="number(substring($decimals_tmp, 2, 1)) + 1"/></xsl:when>
                        <xsl:otherwise><xsl:value-of select="substring($decimals_tmp, 2, 1)"/></xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
                <xsl:variable name="mb_2_dec"><xsl:value-of select="$no_dec_tmp"/>,<xsl:value-of select="substring($decimals_tmp, 1, 1)"/><xsl:value-of select="$last_rounded_dec_tmp"/></xsl:variable>
                <subfield code="z"><xsl:value-of select="$mb_2_dec"/> MB</subfield>
            </xsl:if>
        </datafield>
    </xsl:template>
    
    <!-- controlfield 008 -->
    <xsl:template name="generate_008">
        <xsl:param name="isbn"/>
        <xsl:param name="isbn13"/>
		<xsl:param name="media_type"/>
        <xsl:variable name="first_published_datestamp" select="substring(translate(first_published, '-', ''), 1, 4)"/>
        <xsl:variable name="countrycode">
            <xsl:choose>
                <xsl:when test="starts-with($isbn, '91') or starts-with($isbn13, '97891')">sw</xsl:when>
                <xsl:otherwise>xx</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:variable name="lang_code"><xsl:apply-templates select="language"/></xsl:variable>
		<xsl:variable name="inline_24-34">
			<xsl:choose>
				<xsl:when test="$media_type = 'e-book'">    |||| ||</xsl:when>
				<xsl:when test="$media_type = 'audiobook'">|||||||| | </xsl:when>
			</xsl:choose>
		</xsl:variable>
        <controlfield tag="008"><xsl:value-of select="$datestamp"/>s<xsl:value-of select="$first_published_datestamp"/><xsl:text>    </xsl:text><xsl:value-of select="$countrycode"/><xsl:text> |||| o</xsl:text><xsl:value-of select="$inline_24-34"/><xsl:value-of select="$lang_code"/><xsl:text> d</xsl:text></controlfield>
    </xsl:template>
    
    <!-- updated_date, datafield 599 -->
    <xsl:template match="updated_date">
        <xsl:variable name="formatted_date"><xsl:value-of select="substring(normalize-space(.), 1, 10)"/></xsl:variable>
        <datafield ind1=" " ind2=" " tag="599">
            <subfield code="a">Ändrad av Elib <xsl:value-of select="$formatted_date"/></subfield>
        </datafield>
    </xsl:template>
    
    <!-- title, datafield 245 -->
    <xsl:template match="title">
        <datafield ind1="1" ind2="0" tag="245">
            <subfield code="a"><xsl:value-of select="normalize-space(.)"/></subfield>
            <subfield code="h">[Elektronisk resurs]</subfield>
        </datafield>
    </xsl:template>
    
   <!-- <!-\- id, controlfield 001, datafield 035 -\->
    <xsl:template match="status/id" mode="bibliographic">
        <controlfield tag="001"><xsl:value-of select="normalize-space(.)"/></controlfield>
        <!-\-<datafield ind1=" " ind2=" " tag="035">
            <subfield code="a">Elib<xsl:value-of select="normalize-space(.)"/></subfield>
        </datafield>-\->
    </xsl:template>-->
    
   <!-- <!-\- id, holdings mode, datafield 035 -\->
    <xsl:template match="status/id" mode="holdings">
        <datafield ind1=" " ind2=" " tag="035">
            <subfield code="a">Elib<xsl:value-of select="normalize-space(.)"/></subfield>
        </datafield>
    </xsl:template>-->
    
    <!-- teaser link, datafield 856 -->
    <xsl:template match="teaser/link">
        <xsl:param name="mediatyp"/>
        <datafield ind1=" " ind2="2" tag="856">
            <subfield code="u"><xsl:value-of select="normalize-space(.)"/></subfield>
            <xsl:choose>
                <xsl:when test="$mediatyp = 'e-book'"><subfield code="z">Provläs</subfield></xsl:when>
                <xsl:when test="$mediatyp = 'audiobook'"><subfield code="z">Provlyssna</subfield></xsl:when>
            </xsl:choose>
        </datafield>
    </xsl:template>
    
    <!-- cover_image, datafield 856 -->
    <xsl:template match="coverimage">
        <datafield ind1="4" ind2="2" tag="856">
            <subfield code="u"><xsl:value-of select="normalize-space(.)"/></subfield>
            <subfield code="z">Omslagsbild</subfield>
            <subfield code="x">digipic</subfield>
        </datafield>
    </xsl:template>
    
    <!-- external_id, isbn13/isbn -->
    <xsl:template match="external_ids/external_id/id_type"><xsl:value-of select="normalize-space(../id)"/></xsl:template>
    
    <!-- external_id, datafield 020 -->
    <xsl:template name="external_id">
        <xsl:param name="isbn"/>
        <datafield ind1=" " ind2=" " tag="020">
            <subfield code="a"><xsl:value-of select="$isbn"/></subfield>
        </datafield>
        <controlfield tag="001"><xsl:value-of select="$isbn"/></controlfield>
        <xsl:call-template name="generate_035"><xsl:with-param name="isbn" select="$isbn"/></xsl:call-template>
    </xsl:template>
    
    <!-- generate datafield 035 -->
    <xsl:template name="generate_035">
        <xsl:param name="isbn"/>
        <datafield ind1=" " ind2=" " tag="035">
            <subfield code="a">(Elib)<xsl:value-of select="$isbn"/></subfield>
        </datafield>
    </xsl:template>
    
    <!-- contributors, datafield 100/700 -->
    <xsl:template match="contributors/contributor">
		<xsl:param name="formatgroup"/>
        <xsl:variable name="tag">
            <xsl:choose>
                <xsl:when test="position() = 1 and type='författare'">100</xsl:when>
                <xsl:otherwise>700</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <datafield ind1="1" ind2="" tag="{$tag}">
            <subfield code="a"><xsl:value-of select="normalize-space(family_name)"/><xsl:text>, </xsl:text><xsl:value-of select="normalize-space(first_name)"/></subfield>
                <xsl:if test="$tag = '700' or $tag = '100' and $formatgroup = 'audiobook'">
                    <xsl:choose>
                        <xsl:when test="type = 'författare'"><subfield code="4">aut</subfield></xsl:when>
                        <xsl:when test="type = 'medförfattare'"><subfield code="4">aut</subfield></xsl:when>
                        <xsl:when test="type = 'bearbetare'"><subfield code="4">clb</subfield></xsl:when>
                        <xsl:when test="type = 'fotograf'"><subfield code="4">pht</subfield></xsl:when>
                        <xsl:when test="type = 'illustratör'"><subfield code="4">ill</subfield></xsl:when>
                        <xsl:when test="type = 'ljudeffekter'"><subfield code="4">sds</subfield></xsl:when>
                        <xsl:when test="type = 'lydeffekter'"><subfield code="4">sds</subfield></xsl:when>
                        <xsl:when test="type = 'redaktör'"><subfield code="4">edt</subfield></xsl:when>
                        <xsl:when test="type = 'medredaktör'"><subfield code="4">edt</subfield></xsl:when>
                        <xsl:when test="type = 'musik'"><subfield code="4">cmp</subfield></xsl:when>
                        <xsl:when test="type = 'omslag'"><subfield code="4">cov</subfield></xsl:when>
                        <xsl:when test="type = 'sång'"><subfield code="4">sng</subfield></xsl:when>
                        <xsl:when test="type = 'uppläsare'"><subfield code="4">nrt</subfield></xsl:when>
                        <xsl:when test="type = 'översättare'"><subfield code="4">trl</subfield></xsl:when>
                        <xsl:when test="type = 'övriga medarbetare'"><subfield code="4">oth</subfield></xsl:when>
                        <xsl:otherwise><subfield code="e"><xsl:value-of select="normalize-space(type)"/></subfield></xsl:otherwise>
                    </xsl:choose>
               </xsl:if>
        </datafield>
    </xsl:template>
    
    <!-- publisher name, datafield 260 -->
    <xsl:template match="publisher/name">
        <xsl:variable name="first_published_datestamp" select="substring(translate(../../first_published, '-', ''), 1, 4)"/>
        <datafield ind1="" ind2="" tag="260">
            <subfield code="b"><xsl:value-of select="normalize-space(.)"/><xsl:if test="$first_published_datestamp != ''">,</xsl:if></subfield>
		<xsl:if test="$first_published_datestamp != ''">
            <subfield code="c"><xsl:value-of select="$first_published_datestamp"/></subfield>
		</xsl:if>
        </datafield>
    </xsl:template>
    
    <!-- description, datafield 520 -->
    <xsl:template match="description">
        <datafield ind1="" ind2="" tag="520">
            <subfield code="a"><xsl:call-template name="remove_html_tags"><xsl:with-param name="str" select="normalize-space(.)"></xsl:with-param></xsl:call-template><xsl:text> [Elib]</xsl:text></subfield>
        </datafield>
        <!--
        <datafield ind1="" ind2="" tag="520">
            <xsl:variable name="tmpstr">
                <xsl:call-template name="remove_html_tags"><xsl:with-param name="str" select="normalize-space(.)"></xsl:with-param></xsl:call-template><xsl:text> [Elib]</xsl:text>
            </xsl:variable>
            <xsl:variable name="replaceThis" select="java:java.lang.String.new(string($tmpstr))"/>
            
            <xsl:variable name="ascii-range">("[^\\x20-\\x7e]"</xsl:variable>
            <xsl:variable name="ascii">.,?;:[]!() ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖabcdefghijklmnopqrstuvwxyzåäö0123456789&#x00A0;</xsl:variable>
            <xsl:variable name="asciibort" select="translate($tmpstr, $ascii, '')"/>
            <testing><xsl:value-of select="java:replaceAll($replaceThis, '[^\\x20-\\x7e]', '_')"/></testing>
            <subfield code="a"><xsl:value-of select="translate($tmpstr, ('[^&#x0020;-&#x007e;]', '_')"/></subfield>
        </datafield>
        -->
    </xsl:template>
    
    <!-- categories, datafield 072, 655 -->
    <xsl:template match="categories">
        <xsl:for-each select="category">
            <xsl:choose> 
                <xsl:when test="@type = 'BIC_code'">
                    <datafield ind1=" " ind2="7" tag="072">
                        <subfield code="a"><xsl:value-of select="normalize-space(.)"/></subfield>
                        <subfield code="2">bicssc</subfield>
                    </datafield>
                </xsl:when>
                <xsl:when test="@type = 'Elib_Category'">
                    <xsl:variable name="catid" select="normalize-space(.)"/>
                    <datafield ind1=" " ind2="4" tag="655">
                        <subfield code="a">
                            <xsl:value-of select="document('elibcategories.xml')/*/categoryitem[@id = $catid]"/>
                        </subfield>
                    </datafield>    
                </xsl:when>
            </xsl:choose>
        </xsl:for-each>
    </xsl:template>
    
	<!-- datafield 856 -->
	<xsl:template name="generate_856">
		<xsl:param name="isbn_tmp"/>
		<datafield ind1="4" ind2="0" tag="856">
                  <subfield code="u">http://www.elib.se/library/ebook_detail.asp?id_type=ISBN&amp;ID=<xsl:value-of select="$isbn_tmp"/>&amp;lib=x</subfield>
                  <subfield code="z">Låna från ditt bibliotek via Elib</subfield>
			<subfield code="z">Lånekort krävs</subfield>
          </datafield>
	</xsl:template>

    <!-- language, translate ISO 639-1 code to ISO 639-2B code -->
    <xsl:template match="language">
        <xsl:choose>
            <xsl:when test="normalize-space(.) = 'sv'">swe</xsl:when>
            <xsl:when test="normalize-space(.) = 'en'">eng</xsl:when>
            <xsl:when test="normalize-space(.) = 'de'">ger</xsl:when>
            <xsl:when test="normalize-space(.) = 'fr'">fre</xsl:when>
            <xsl:when test="normalize-space(.) = 'da'">dan</xsl:when>
            <xsl:when test="normalize-space(.) = 'it'">ita</xsl:when>
            <xsl:when test="normalize-space(.) = 'fi'">fin</xsl:when>
            <xsl:when test="normalize-space(.) = 'nl'">dut</xsl:when>
            <xsl:when test="normalize-space(.) = 'ru'">rus</xsl:when>
            <xsl:when test="normalize-space(.) = 'es'">spa</xsl:when>
            <xsl:when test="normalize-space(.) = 'pl'">pol</xsl:when>
            <xsl:when test="normalize-space(.) = 'no'">nor</xsl:when> 
            <xsl:when test="normalize-space(.) = 'hu'">hun</xsl:when>
            <xsl:when test="normalize-space(.) = 'cs'">cze</xsl:when>
            <xsl:when test="normalize-space(.) = 'pt'">por</xsl:when>
            <xsl:when test="normalize-space(.) = 'ja'">jpn</xsl:when>
            <xsl:when test="normalize-space(.) = 'ku'">kur</xsl:when>
            <xsl:when test="normalize-space(.) = 'fa'">per</xsl:when>
            <xsl:when test="normalize-space(.) = 'ar'">ara</xsl:when>
            <xsl:when test="normalize-space(.) = 'tr'">tur</xsl:when>
            <xsl:when test="normalize-space(.) = 'sl'">slv</xsl:when>
            <xsl:when test="normalize-space(.) = 'et'">est</xsl:when>
            <xsl:when test="normalize-space(.) = 'lv'">lav</xsl:when>
            <xsl:when test="normalize-space(.) = 'hr'">hrv</xsl:when>
            <xsl:when test="normalize-space(.) = 'la'">lat</xsl:when>
            <xsl:when test="normalize-space(.) = 'so'">som</xsl:when>
            <xsl:when test="normalize-space(.) = 'el'">gre</xsl:when>
            <xsl:when test="normalize-space(.) = 'se'">sme</xsl:when>
            <xsl:when test="normalize-space(.) = 'eo'">epo</xsl:when>
        </xsl:choose>
    </xsl:template>
    
    <!-- remove html tags from str -->
    <xsl:template name="remove_html_tags">
        <xsl:param name="str"/>
        <xsl:choose>
            <xsl:when test="contains($str, '&lt;')">
                <xsl:value-of select="substring-before($str, '&lt;')"/>
                <!-- recurse through str -->
                <xsl:call-template name="remove_html_tags">
                    <xsl:with-param name="str" select="substring-after($str, '&gt;')"/>
                </xsl:call-template>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="$str"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template> 
    
</xsl:stylesheet>
