<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns="http://www.loc.gov/MARC21/slim" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" exclude-result-prefixes="dc">
	<xsl:output method="xml" indent="yes"/>
	
	<xsl:template match="/">
		<record type="Bibliographic" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd" >
			<xsl:element name="leader">
				<xsl:variable name="type" select="dc:type"/>
				<xsl:variable name="leader06">
					<xsl:choose>
						<xsl:when test="$type='collection'">p</xsl:when>
						<xsl:when test="$type='dataset'">m</xsl:when>
						<xsl:when test="$type='event'">r</xsl:when>
						<xsl:when test="$type='image'">k</xsl:when>
						<xsl:when test="$type='interactive resource'">m</xsl:when>
						<xsl:when test="$type='service'">m</xsl:when>
						<xsl:when test="$type='software'">m</xsl:when>
						<xsl:when test="$type='sound'">i</xsl:when>
						<xsl:when test="$type='text'">a</xsl:when>
						<xsl:otherwise>a</xsl:otherwise>
					</xsl:choose>
				</xsl:variable>
				<xsl:variable name="leader07">
					<xsl:choose>
						<xsl:when test="$type='collection'">c</xsl:when>
						<xsl:otherwise>m</xsl:otherwise>
					</xsl:choose>
				</xsl:variable>
				<xsl:value-of select="concat('      ',$leader06,$leader07,'         3u     ')"/>
			</xsl:element>

			<datafield tag="042" ind1=" " ind2=" ">
				<subfield code="a">dc</subfield>
			</datafield>

                        <xsl:for-each select="//dc:creator[1]">
				<datafield tag="100" ind1=" " ind2=" ">
					<subfield code="a">
						<xsl:value-of select="."/>
					</subfield>
					<subfield code="e">author</subfield>
				</datafield>
			</xsl:for-each>

			<xsl:for-each select="//dc:title[1]">
				<datafield tag="245" ind1="0" ind2="0">
					<subfield code="a">
						<xsl:value-of select="."/>
					</subfield>
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
                            <subfield code="a">Luleå</subfield>
                            <xsl:if test="//dc:publisher">
                                    <subfield code="b">
                                            <xsl:value-of select="//dc:publisher[1]"/>
                                    </subfield>
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

                        <xsl:for-each select="//dc:creator[position()>1]">
				<datafield tag="700" ind1=" " ind2=" ">
					<subfield code="a">
						<xsl:value-of select="."/>
					</subfield>
					<subfield code="e">author</subfield>
				</datafield>
			</xsl:for-each>

			<xsl:for-each select="//dc:identifier">
				<datafield tag="856" ind1=" " ind2=" ">
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
                     
                    <!-- 852 -->
                    <datafield tag="852" ind1=" " ind2=" ">
                        <subfield code="b">La</subfield>
                        <subfield code="h">internet</subfield>
                    </datafield>
                </record>
	</xsl:template>
</xsl:stylesheet>