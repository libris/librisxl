<?xml version="1.0" encoding="UTF-8" ?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="xml"/>
    <xsl:param name="f008_00-05">050420</xsl:param>

    <xsl:template match="/EBOKFIL">      
        <collection xmlns="http://www.loc.gov/MARC21/slim">
            <xsl:for-each select="EBOKARTIKEL">
                <xsl:variable name="lang">
                    <xsl:choose>
                        <xsl:when test="SPRAK = 'se'">swe</xsl:when>
                        <xsl:when test="SPRAK = 'en'">eng</xsl:when>
                        <xsl:when test="SPRAK = 'de'">ger</xsl:when>
                        <xsl:otherwise>swe</xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
                <!-- Bibliographic record -->            
                <record type="Bibliographic">
                    <!-- leader -->
                    <leader>#####nam  22#####5a 4500</leader>
                    
                    <!-- 007 -->
                    <controlfield tag="007">cr||||||||||||</controlfield>
                    
                    <!-- 008 -->
                    <controlfield tag="008"><xsl:value-of select="$f008_00-05"/>s<xsl:value-of select="substring(normalize-space(UTGIVNINGSAR), 0, 5)"/>    sw ||||_s____|00| 0_<xsl:value-of select="normalize-space($lang)"/>__</controlfield>
                    
                    <!-- 020/022 -->                    
                    <xsl:choose>
                        <xsl:when test="IDTYP = 'ISBN' or IDTYPE = 'ISBN'">
                            <datafield tag="020" ind1="" ind2="">
                                <subfield code="a"><xsl:value-of select="ID"/></subfield>
                            </datafield>
                        </xsl:when>
                        <xsl:when test="IDTYP = 'ISSN' or IDTYPE = 'ISSN'">
                            <datafield tag="022" ind1="" ind2="">
                                <subfield code="a"><xsl:value-of select="ID"/></subfield>
                            </datafield>
                        </xsl:when>
                    </xsl:choose>
                    
                    <!-- 040 -->
                    <datafield tag="040" ind1="" ind2="">
                        <subfield code="a">LGEN</subfield>
                    </datafield>
                    
                    <!-- 042 -->
                    <datafield tag="042" ind1="" ind2="">
                        <subfield code="9">NB</subfield>
                    </datafield>
                    
                    <!-- 100 -->
                    <xsl:for-each select="FORFATTARE[@typ = 'forfattare']">
                        <datafield tag="100" ind1="1" ind2="">
                            <subfield code="a"><xsl:value-of select="normalize-space(EFTERNAMN)"/>, <xsl:value-of select="normalize-space(FORNAMN)"/></subfield>
                        </datafield>
                    </xsl:for-each>
                    
                    <!-- 245 -->
                    <datafield tag="245" ind1="1" ind2="0">
                        <subfield code="a"><xsl:value-of select="normalize-space(TITEL)"/></subfield>
                        <subfield code="h">[Elektronisk resurs]</subfield>
                    </datafield>
                    
                    <!-- 256 -->
                    <datafield tag="256" ind1="" ind2="">
                        <subfield code="a">Text</subfield>
                    </datafield>
                    
                    <!-- 260 -->
                    <datafield tag="260" ind1="" ind2="">
                        <subfield code="a"><xsl:value-of select="normalize-space(FORLAG/ORT)"/> :</subfield>
                        <subfield code="b"><xsl:value-of select="normalize-space(FORLAG/NAMN)"/>,</subfield>
                        <subfield code="c"><xsl:value-of select="normalize-space(UTGIVNINGSAR)"/></subfield>
                    </datafield>
                    
                    <!-- 500 -->
                    <datafield tag="500" ind1="" ind2="">
                        <subfield code="a">Titel från titelskärmbild</subfield>
                    </datafield>
                    
                    <!-- 538 -->
                    <datafield tag="538" ind1="" ind2="">
                        <subfield code="a">E-bok</subfield>
                    </datafield>
                    
                    <!-- 700 -->
                    <xsl:for-each select="FORFATTARE[TYP = '2']">
                        <datafield tag="700" ind1="1" ind2="">
                            <subfield code="a"><xsl:value-of select="normalize-space(EFTERNAMN)"/>, <xsl:value-of select="normalize-space(FORNAMN)"/></subfield>
                        </datafield>
                    </xsl:for-each>

                    <!-- 856 -->
                    <datafield tag="856" ind1="4" ind2="2">
                        <subfield code="u">http://www.omnibus.se/eBoklagret</subfield>
                        <subfield code="z">Eboklagret/Omnibus webbplats</subfield>
                    </datafield>
                </record>

                <!-- Holdings record -->
                <record type="Holdings">
                    <!-- leader -->
                    <leader>#####nx   22#####1n 4500</leader>
                    
                    <!-- 008 -->                    
                    <controlfield tag="008"><xsl:value-of select="$f008_00-05"/>||0000|||||001||||||000000</controlfield>

                    <!-- 852 -->
                    <datafield tag="852" ind1="" ind2="">
                        <subfield code="b">S</subfield>
                        <subfield code="h">RefKB</subfield>
                        <subfield code="j">E-bok</subfield>
                    </datafield>                                                            

                    <!-- 856 -->
                    <datafield tag="856" ind1="4" ind2="0">
                        <subfield code="u">http://ebok.kb.se/<xsl:value-of select="/EBOKFIL/EBOKARTIKEL/FILENAME"/></subfield>
                        <subfield code="z">Lokalt tillgänglig på KB</subfield>
                    </datafield>
                </record>
            </xsl:for-each>
        </collection>
    </xsl:template>
</xsl:stylesheet> 
