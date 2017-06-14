<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:marc="http://www.loc.gov/MARC21/slim"
    exclude-result-prefixes="marc">
    <xsl:output method="xml" omit-xml-declaration="no" indent="yes"/>
    <xsl:template match="/">
        <xsl:apply-templates select="merge"/>
    </xsl:template>
    <xsl:template match="merge">
        <collection xmlns="http://www.loc.gov/MARC21/slim">
            <record type="Holdings">
                
                <!-- include all fields from old record -->
                <xsl:apply-templates select="old_record/marc:record[@type='Holdings']/marc:leader"/>
                <xsl:apply-templates select="old_record/marc:record[@type='Holdings']/marc:controlfield"/>
                <xsl:apply-templates select="old_record/marc:record[@type='Holdings']/marc:datafield[@tag != '024' and @tag != '852' and @tag != '856']"/>
                
                <!-- include fields 024,948 and 951 from new record if they do not exist in old record -->
                <!--<xsl:if test="count(old_record/marc:record[@type='Holdings']/marc:datafield[@tag = '024']) = 0">
                    <xsl:apply-templates select="new_record/marc:record[@type='Holdings']/marc:datafield[@tag = '024']"/>
                </xsl:if>-->

                <xsl:if test="count(old_record/marc:record[@type='Holdings']/marc:datafield[@tag = '948']) = 0">
                    <xsl:apply-templates select="new_record/marc:record[@type='Holdings']/marc:datafield[@tag = '948']"/>
                </xsl:if>
                <xsl:if test="count(old_record/marc:record[@type='Holdings']/marc:datafield[@tag = '951']) = 0">
                    <xsl:apply-templates select="new_record/marc:record[@type='Holdings']/marc:datafield[@tag = '951']"/>
                </xsl:if>           
                
		<!-- merge of 024, 852, 856 rewritten by Kai P -->
		<!-- 150209 modified KP, deleted holdings have no 024,056 -->
		<!-- therefore, origin is taken from 852x. -->
		<!-- also, removal of old_record 024,056 -->

		<!-- 024 -->
		<xsl:variable name="origin" select="normalize-space(substring-after(string(/merge/new_record/marc:record[@type='Holdings']/marc:datafield[@tag='852']/marc:subfield[@code='x' and starts-with(./text(), 'origin:')]), 'origin:'))"/>
		<xsl:apply-templates select="old_record/marc:record[@type='Holdings']/marc:datafield[@tag = '024' and not(./marc:subfield[@code='2' and ./text() =  concat('Distributör: ', $origin)])]"/>
                <xsl:apply-templates select="new_record/marc:record[@type='Holdings']/marc:datafield[@tag='024']"/>
		<!-- 852x -->
		<xsl:apply-templates select="old_record/marc:record[@type='Holdings']/marc:datafield[@tag = '852' and not(./marc:subfield[@code='x' and ./text() = concat('origin:', $origin)])]"/>
                <xsl:apply-templates select="new_record/marc:record[@type='Holdings']/marc:datafield[@tag='852']"/>
                
		<!-- 856x -->
		<xsl:apply-templates select="old_record/marc:record[@type='Holdings']/marc:datafield[@tag = '856' and not(./marc:subfield[@code='x' and ( ./text() = concat('origin:', $origin) or ./text() = concat('origin: ', $origin) )])]"/> <!-- fix for false merges -->
                <xsl:apply-templates select="new_record/marc:record[@type='Holdings']/marc:datafield[@tag='856']"/>

             </record>
        </collection>
    </xsl:template>
    
    <!-- Copy template -->
    <xsl:template match="@* | node()">
        <xsl:copy>
            <xsl:apply-templates select="@* | node()"/>
        </xsl:copy>
    </xsl:template>
</xsl:stylesheet>

