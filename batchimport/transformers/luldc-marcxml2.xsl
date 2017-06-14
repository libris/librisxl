<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns="http://www.loc.gov/MARC21/slim" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" exclude-result-prefixes="dc">
	<xsl:output method="xml" indent="yes"/>
        <xsl:param name="f008_00-05">050420</xsl:param>
	
	<xsl:template match="/">
		<record type="Bibliographic" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd" >
			<xsl:element name="leader">
				<xsl:value-of select="concat('     nam  22     3a 4500')"/>
			</xsl:element>

			<!-- 007 -->
			<controlfield tag="007">cr||||||||||||</controlfield>

			<!-- 008 -->
			<xsl:element name="controlfield">
				<xsl:attribute name="tag">008</xsl:attribute>
				<xsl:value-of select="concat($f008_00-05,'s','    ','    ','sw ','||||','       ','00','|||',' ','||','SPR',' ','c')"/>
			</xsl:element>

			<datafield tag="040" ind1=" " ind2=" ">
				<subfield code="a">La</subfield>
			</datafield>

			<datafield tag="042" ind1=" " ind2=" ">
				<subfield code="a">dc</subfield>
			</datafield>

                        <xsl:for-each select="//dc:creator[1]">
				<datafield tag="100" ind1="1" ind2=" ">
					<subfield code="a">
						<xsl:value-of select="."/>
					</subfield>
				</datafield>
			</xsl:for-each>

			<xsl:for-each select="//dc:title[1]">
				<datafield tag="245" ind1="1" ind2="0">
					<subfield code="a">
						<xsl:value-of select="."/>
					</subfield>
					<subfield code="h">[Elektronisk resurs]</subfield>
				</datafield>
			</xsl:for-each>

			<xsl:for-each select="//dc:title[position()>1]">
				<datafield tag="246" ind1="3" ind2="3">
					<subfield code="a">
						<xsl:value-of select="."/>
					</subfield>
				</datafield>
			</xsl:for-each>

                        <datafield tag="260" ind1=" " ind2=" ">
                            <subfield code="a">Luleè¿ª :</subfield>
                            <xsl:if test="//dc:publisher">
                                    <subfield code="b"><xsl:value-of select="//dc:publisher[1]"/>,</subfield>
                            </xsl:if>	
                            <xsl:if test="//dc:date">
                                    <subfield code="c">
                                            <xsl:value-of select="//dc:date[1]"/>
                                    </subfield>
                            </xsl:if>	
                        </datafield>
                        
			<xsl:for-each select="//dc:subject">
				<datafield tag="653" ind1=" " ind2=" ">
					<subfield code="a">
						<xsl:value-of select="."/>
					</subfield>
				</datafield>
			</xsl:for-each>

			<xsl:for-each select="//dc:type[. != 'A' and . != 'B' and . != 'C' and . != 'D' and . != 'Y']>
				<datafield tag="655" ind1="" ind2="">
					<subfield code="a">
						<xsl:value-of select="."/>
					</subfield>
				</datafield>
			</xsl:for-each>

                        <xsl:for-each select="//dc:creator[position()>1]">
				<datafield tag="700" ind1=" " ind2=" ">
					<subfield code="a">
						<xsl:value-of select="."/>
					</subfield>
				</datafield>
			</xsl:for-each>

			<xsl:for-each select="//dc:identifier">
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
		    <controlfield tag="008"><xsl:value-of select="concat($f008_00-05,'||____|||||001||||||000000')"/></controlfield>
                    
                    <!-- 852 -->
                    <datafield tag="852" ind1=" " ind2=" ">
                        <subfield code="b">La</subfield>
                        <subfield code="h">internet</subfield>
                    </datafield>
                </record>
	</xsl:template>
</xsl:stylesheet>
