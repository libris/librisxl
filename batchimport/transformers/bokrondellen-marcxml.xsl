<?xml version="1.0" encoding="UTF-8" ?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

    <xsl:param name="f008_00-05">050420</xsl:param>

    <xsl:template match="/artikelregister">
        <collection xmlns="http://www.loc.gov/MARC21/slim">
            <xsl:for-each select="artikel">
                <xsl:variable name="lang">
                    <xsl:choose>
                        <xsl:when test="sprak = 'Svenska'">swe</xsl:when>
                        <xsl:when test="sprak = 'Engelska'">eng</xsl:when>
                        <xsl:when test="sprak = 'Tyska'">ger</xsl:when>
                        <xsl:otherwise>swe</xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
                <xsl:variable name="f008_06-10">
                    <xsl:choose>
                        <xsl:when test="utgivningsdatum = 0">|||||</xsl:when>
                        <xsl:otherwise>s<xsl:value-of select="substring(utgivningsdatum, 1, 4)"/></xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
 
                <!-- Bibliographic record -->            
                <record type="Bibliographic">
                    <!-- leader -->
                    <leader>#####nam  22#####8a 4500</leader>
                    
                    <!-- 008 -->
                    <controlfield tag="008"><xsl:value-of select="$f008_00-05"/><xsl:value-of select="$f008_06-10"/>    sw ||||      |00| 0|<xsl:value-of select="normalize-space($lang)"/>__</controlfield>
                    <!-- 020/022 -->                    
                    <xsl:choose>
                        <xsl:when test="string-length(artikelnummer) = 10 or string-length(artikelnummer) = 13">
                            <datafield tag="020" ind1=" " ind2=" ">
                                <subfield code="a"><xsl:value-of select="artikelnummer"/></subfield>
                            </datafield>
                        </xsl:when>
                        <xsl:when test="string-length(artikelnummer) = 8">
                            <datafield tag="022" ind1=" " ind2=" ">
                                <subfield code="a"><xsl:value-of select="artikelnummer"/></subfield>
                            </datafield>
                        </xsl:when>
                    </xsl:choose>
                    
                    <!-- 040 -->
                    <datafield tag="040" ind1="" ind2="">
                        <subfield code="a">LGEN</subfield>
                    </datafield>
                    
                    <!-- 100 -->
                    <xsl:for-each select="medarbetare[@typ = 'forfattare']">
                        <xsl:if test="position() = 1">
                        <datafield tag="100" ind1="1" ind2=" ">
                            <subfield code="a"><xsl:value-of select="."/></subfield>
                        </datafield>
                        </xsl:if>
                    </xsl:for-each>
                    
                    <!-- 245 -->
                    <datafield tag="245" ind1="1" ind2="0">
                        <xsl:choose>
                            <xsl:when test="titel and titel != ''">
                                <subfield code="a"><xsl:value-of select="titel"/></subfield>
                            </xsl:when>
                            <xsl:when test="arbetstitel and arbetstitel != ''">
                                <subfield code="a"><xsl:value-of select="arbetstitel"/></subfield>
                            </xsl:when>
                            <xsl:otherwise>
                                <subfield code="a">[Titel saknas]</subfield>
                            </xsl:otherwise>        
                        </xsl:choose>
                    </datafield>
                    
                    <!-- 260 -->
                    <datafield tag="260" ind1=" " ind2=" ">
                        <subfield code="b"><xsl:value-of select="forlag"/>,</subfield>
                        <subfield code="c"><xsl:value-of select="substring(utgivningsdatum, 1, 4)"/></subfield>
                    </datafield>

                    <!-- 263 -->
                    <xsl:if test="utgivningsdatum and utgivningsdatum != ''">
                    <datafield tag="263" ind1=" " ind2=" ">
                        <subfield code="a"><xsl:value-of select="substring(utgivningsdatum, 1, 6)"/></subfield>
                    </datafield>
	            </xsl:if>

                    <!-- 700 -->
                    <xsl:for-each select="medarbetare[@typ = 'forfattare']">
                        <xsl:if test="position() != 1">
                        <datafield tag="700" ind1="1" ind2=" ">
                            <subfield code="a"><xsl:value-of select="."/></subfield>
                        </datafield>
                        </xsl:if>
                    </xsl:for-each>
                </record>

                <!-- Holdings record -->
                <record type="Holdings">
                    <!-- leader -->
                    <leader>#####nx   22#####1n 4500</leader>
                    
                    <!-- 008 -->
                    <controlfield tag="008"><xsl:value-of select="$f008_00-05"/>||0000|||||001||||||000000</controlfield>

                    <!-- 852 -->
                    <datafield tag="852" ind1=" " ind2=" ">
                        <subfield code="b">BOKR</subfield>
                    </datafield>                    
                </record>
            </xsl:for-each>
        </collection>
    </xsl:template>
</xsl:stylesheet> 
