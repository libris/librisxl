<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0"
    xmlns:oai="http://www.openarchives.org/OAI/2.0/"
    xmlns:dc="http://purl.org/dc/elements/1.1/"
    xmlns="http://www.loc.gov/MARC21/slim"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
    exclude-result-prefixes="dc">
	<xsl:output method="xml" indent="yes"/>
        <xsl:param name="f008_00-05">050420</xsl:param>
	
        <xsl:template match="/">
            <xsl:apply-templates select="/oai:OAI-PMH"/>
        </xsl:template>
        
        <xsl:template match="oai:OAI-PMH">
            <xsl:apply-templates select="oai:ListRecords"/>
        </xsl:template>
        
        <xsl:template match="oai:ListRecords">
            <collection xmlns="http://www.loc.gov/MARC21/slim">
            <xsl:apply-templates select="oai:record"/>
            </collection>
        </xsl:template>

        <xsl:template match="oai:record">
            <xsl:apply-templates select="oai:metadata"/>            
        </xsl:template>
	
        <xsl:template match="oai:metadata">
            <xsl:apply-templates select="oai_dc:dc"/>            
        </xsl:template>
	
        <xsl:template match="oai_dc:dc">
            <xsl:variable name="issn"><xsl:value-of select="substring(dc:title[2], string-length(dc:title[2]) - 8, 9)"/></xsl:variable>
            
            <!--<xsl:if test="$issn = '1402-1773' or $issn = '1402-1781' or $issn = '1402-1552' or $issn = '1653-0187' or $issn = '1652-5299' or $issn = '1402-1595' or $issn = '1404-5508' or $issn = '1402-697X' or $issn = '1402-1560'">-->
		<record type="Bibliographic" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd" >
                        <xsl:variable name="year">
                            <xsl:choose>
                                <xsl:when test="dc:date[1] and string-length(dc:date[1]) >= 4">
                                    <xsl:value-of select="substring(dc:date[1], 1, 4)"/>
                                </xsl:when>
                                <xsl:otherwise><xsl:value-of select="string('    ')"/></xsl:otherwise>
                            </xsl:choose>
                        </xsl:variable>
                        
                        <xsl:variable name="lang">
                            <xsl:choose>
                                <xsl:when test="dc:language = 'eng'">eng</xsl:when>
                                <xsl:when test="dc:language = 'ger'">ger</xsl:when>
                                <xsl:when test="dc:language = 'fra'">fra</xsl:when>
                                <xsl:when test="dc:language = 'spa'">spa</xsl:when>
                                <xsl:when test="dc:language = 'swe'">swe</xsl:when>
                                <xsl:otherwise>und</xsl:otherwise>
                            </xsl:choose>
                        </xsl:variable>                                                
                    
                        <xsl:element name="leader">
				<xsl:value-of select="concat('     nam  22     3a 4500')"/>
			</xsl:element>

			<!-- 007 -->
			<controlfield tag="007">cr||||||||||||</controlfield>

			<!-- 008 -->
			<xsl:element name="controlfield">
				<xsl:attribute name="tag">008</xsl:attribute>
				<xsl:value-of select="concat($f008_00-05, 's', $year, '    ', 'sw ', '||||', '     ', ' |', '00|', ' ', '||', $lang, ' ', 'c')"/>
			</xsl:element>

			<datafield tag="035" ind1=" " ind2=" ">
				<subfield code="a"><xsl:value-of select="../../oai:header/oai:identifier"/></subfield>
			</datafield>

			<datafield tag="040" ind1=" " ind2=" ">
				<subfield code="a">La</subfield>
			</datafield>

			<datafield tag="042" ind1=" " ind2=" ">
				<subfield code="a">dc</subfield>
			</datafield>

                        <xsl:for-each select="dc:creator[1]">
				<datafield tag="100" ind1="1" ind2=" ">
					<subfield code="a">
						<xsl:value-of select="."/>
					</subfield>
				</datafield>
			</xsl:for-each>

			<xsl:for-each select="dc:title[1]">
				<datafield tag="245" ind1="1" ind2="0">
					<subfield code="a">
						<xsl:value-of select="."/>
					</subfield>
					<subfield code="h">[Elektronisk resurs]</subfield>
				</datafield>
			</xsl:for-each>

                        <datafield tag="260" ind1=" " ind2=" ">
                            <subfield code="a">Luleå :</subfield>
                            <xsl:if test="dc:publisher">
                                    <subfield code="b"><xsl:value-of select="dc:publisher[1]"/>,</subfield>
                            </xsl:if>	
                            <xsl:if test="dc:date">
                                    <subfield code="c">
                                            <xsl:value-of select="dc:date[1]"/>
                                    </subfield>
                            </xsl:if>	
                        </datafield>

                        <xsl:for-each select="dc:title[2]">
                            <xsl:choose>
                                <xsl:when test="$issn != ''">
                                    <datafield tag="440" ind1=" " ind2="0">
                                        <subfield code="a">
                                            <xsl:value-of select="substring(., 1, string-length(.)-9)"/>
                                        </subfield>
                                        <subfield code="x">
                                            <xsl:value-of select="$issn"/>
                                        </subfield>
                                    </datafield>
                                </xsl:when>
                                <xsl:otherwise>
                                    <datafield tag="440" ind1=" " ind2="0">
                                        <subfield code="a">
                                            <xsl:value-of select="."/>
                                        </subfield>
                                    </datafield>
                                </xsl:otherwise>
                            </xsl:choose>
			</xsl:for-each>

                        <xsl:for-each select="dc:type[. = 'A' or . = 'B' or . = 'C' or . = 'D']">
                            <datafield tag="502" ind1="" ind2="">
                                <subfield code="a"><xsl:value-of select="."/>-uppsats</subfield>
                            </datafield>
                        </xsl:for-each>
                        
                        <xsl:for-each select="dc:type[. = 'Y']">
                            <datafield tag="502" ind1="" ind2="">
                                    <subfield code="a">Examensarbete</subfield>
                            </datafield>
                        </xsl:for-each>
                        
			<xsl:for-each select="dc:subject">
                            <datafield tag="653" ind1=" " ind2=" ">
                                    <subfield code="a">
                                            <xsl:value-of select="."/>
                                    </subfield>
                            </datafield>
			</xsl:for-each>

                        <xsl:for-each select="dc:creator[position()>1]">
                            <datafield tag="700" ind1="1" ind2=" ">
                                    <subfield code="a">
                                            <xsl:value-of select="."/>
                                    </subfield>
                            </datafield>
			</xsl:for-each>

			<xsl:for-each select="dc:identifier">
                            <datafield tag="856" ind1="4" ind2="0">
                                <subfield code="u">
                                        <xsl:value-of select="."/>
                                </subfield>
                            </datafield>
			</xsl:for-each>
                </record>

                <!-- Holdings record -->
                <record type="Holdings" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">
                    <!-- leader -->
                    <leader>#####nx   22#####1n 4500</leader>
                    
		    <!-- 008 -->
		    <controlfield tag="008"><xsl:value-of select="concat($f008_00-05,'||    |||||001||||||000000')"/></controlfield>
                    
                    <!-- 852 -->
                    <datafield tag="852" ind1=" " ind2=" ">
                        <subfield code="b">La</subfield>
                        <subfield code="h">internet</subfield>
                    </datafield>
                </record>
            <!--</xsl:if>-->
	</xsl:template>       
</xsl:stylesheet>
