<?xml version="1.0" encoding="UTF-8" ?>
<xsl:stylesheet version="1.0"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:marc="http://www.loc.gov/MARC21/slim">
    
    <xsl:template match="/marc:collection">
        <collection xmlns="http://www.loc.gov/MARC21/slim">
            <xsl:apply-templates select="marc:record"/>
        </collection>
    </xsl:template>
    
    <xsl:template match="marc:record">
        <xsl:variable name="f008"><xsl:value-of select="normalize-space(marc:controlfield[@tag = '008'])"/></xsl:variable>
        <xsl:variable name="f096a"><xsl:value-of select="normalize-space(marc:datafield[@tag = '096']/marc:subfield[@code = 'a'])"/></xsl:variable>

        <xsl:if test="substring($f096a, 1, 2) &lt; 50">
        <!-- bibliographic record -->
        <record type="Bibliographic">
            <!-- leader -->
            <leader>#####nam  22#####8a 4500</leader>
            
            <!-- controlfield 008 -->
            <controlfield tag="008"><xsl:value-of select="substring($f008, 1,6)"/>s20<xsl:value-of select="substring($f096a, 1, 2)"/>    sw ||||      |00| 0|swe  </controlfield>
            
            <!-- datafield 020 -->
            <datafield tag="020" ind1=" " ind2=" ">
                <xsl:choose>
                    <xsl:when test="substring(marc:controlfield[@tag = '001'], 1, 4) = '9149'">
                        <subfield code="z"><xsl:value-of select="normalize-space(marc:controlfield[@tag = '001'])"/></subfield>
                    </xsl:when>
                    <xsl:otherwise>
                        <subfield code="a"><xsl:value-of select="normalize-space(marc:controlfield[@tag = '001'])"/></subfield>
                    </xsl:otherwise>
                </xsl:choose>
            </datafield>
            
            <!-- datafield 040 -->
            <datafield tag="040" ind1=" " ind2=" ">
                <subfield code="a">LGEN</subfield>
            </datafield>
            
            <!-- datafield 100 -->
            <xsl:apply-templates select="marc:datafield[@tag = '100']"/>
                
            <!-- datafield 245 -->
            <datafield tag="245" ind1="1" ind2="0">
                <subfield code="a"><xsl:value-of select="normalize-space(marc:datafield[@tag = '245']/marc:subfield[@code = 'a'])"/></subfield>
            </datafield>
            
            <!-- datafield 250 -->
            <xsl:if test="marc:datafield[@tag = '250']">
            <datafield tag="250" ind1=" " ind2=" ">
                <subfield code="a"><xsl:value-of select="normalize-space(marc:datafield[@tag = '250']/marc:subfield[@code = 'a'])"/></subfield>
            </datafield>
            </xsl:if>

            <!-- datafield 260 -->
            <datafield tag="260" ind1=" " ind2=" ">
                <subfield code="b"><xsl:value-of select="normalize-space(marc:datafield[@tag = '260']/marc:subfield[@code = 'b'])"/>,</subfield>
                <subfield code="c">20<xsl:value-of select="substring($f096a, 1, 2)"/></subfield>
            </datafield>
            
            <!-- datafield 263 -->
            <xsl:apply-templates select="marc:datafield[@tag = '096']"/>
        </record>
        
        <!-- holdings record -->
        <record type="Holdings">
            <!-- leader -->
            <leader>#####nx   22#####1n 4500</leader>
            
            <!-- 008 -->
            <controlfield tag="008"><xsl:value-of select="substring($f008, 1,6)"/>||0000|||||001||||||000000</controlfield>

            <!-- datafield 852 -->
            <datafield tag="852" ind1=" " ind2=" ">
                <subfield code="b">SEE</subfield>
            </datafield>
        </record>
        </xsl:if>
    </xsl:template>
    
    <xsl:template match="datafield[@tag = '096']">
<!--        
        <datafield tag="042" ind1=" " ind2=" ">
            <subfield code="9"><xsl:value-of select="normalize-space(subfield[@code = 'y'])"/></subfield>
        </datafield>
-->
        <datafield tag="263" ind1=" " ind2=" ">
            <subfield code="a"><xsl:value-of select="normalize-space(marc:subfield[@code = 'a'])"/></subfield>
        </datafield>
    </xsl:template>

    <xsl:template match="marc:datafield[@tag = '100']">
        <datafield tag="100">
            <xsl:attribute name="ind1">
                <xsl:choose>
                    <xsl:when test="@ind1 = 'e'">1</xsl:when>
                    <xsl:when test="@ind1 = 'f'">0</xsl:when>
                    <xsl:when test="@ind1 = '2'">1</xsl:when>
                    <xsl:otherwise>1</xsl:otherwise>                    
                </xsl:choose>
            </xsl:attribute>
            <xsl:attribute name="ind2">
                <xsl:choose>
                    <xsl:when test="@ind1 = 'n'">3</xsl:when>
                    <xsl:otherwise> </xsl:otherwise>                    
                </xsl:choose>
            </xsl:attribute>
            <subfield code="a">
                <xsl:value-of select="normalize-space(marc:subfield[@code = 'a'])"/>
            </subfield>
        </datafield>
    </xsl:template>
</xsl:stylesheet> 
