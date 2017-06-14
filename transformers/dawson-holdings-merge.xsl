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
            <!--<xsl:copy-of select="new_record/marc:record[@type='Holdings']"/>-->
            <record type="Holdings">
                <xsl:apply-templates select="old_record/marc:record[@type='Holdings']/marc:leader"/>
                <xsl:apply-templates select="old_record/marc:record[@type='Holdings']/marc:controlfield"/>
                <xsl:apply-templates select="old_record/marc:record[@type='Holdings']/marc:datafield[@tag &lt;= '852']"/>
                <xsl:apply-templates select="new_record/marc:record[@type='Holdings']/marc:datafield[@tag = '852']" mode="checkdouble"/>
                <xsl:apply-templates select="old_record/marc:record[@type='Holdings']/marc:datafield[@tag > '852' and @tag &lt; '856']"/>
                <xsl:apply-templates select="old_record/marc:record[@type='Holdings']/marc:datafield[@tag = '856' and count(marc:subfield[@code='z' and normalize-space(.) = 'Dawsonera']) = 0]"/>
                <xsl:apply-templates select="new_record/marc:record[@type='Holdings']/marc:datafield[@tag = '856']"/>
                <xsl:apply-templates select="old_record/marc:record[@type='Holdings']/marc:datafield[@tag > '856']"/>
                <xsl:if test="count(old_record/marc:record[@type='Holdings']/marc:datafield[@tag = '948']) = 0">
                    <xsl:apply-templates select="new_record/marc:record[@type='Holdings']/marc:datafield[@tag = '948']"/>
                </xsl:if>
                <xsl:if test="count(old_record/marc:record[@type='Holdings']/marc:datafield[@tag = '951']) = 0">
                    <xsl:apply-templates select="new_record/marc:record[@type='Holdings']/marc:datafield[@tag = '951']"/>
                </xsl:if>
            </record>
        </collection>
    </xsl:template>
    <xsl:template match="new_record/marc:record[@type='Holdings']/marc:datafield[@tag='852']" mode="checkdouble">
        <xsl:variable name="old852b" select="normalize-space(marc:subfield[@code='b'])"/>
        <xsl:variable name="old852h" select="normalize-space(marc:subfield[@code='h'])"/>
        <xsl:if test="not(../../../old_record/marc:record[@type='Holdings']/marc:datafield[@tag='852' and normalize-space(marc:subfield[@code='b']) = $old852b and normalize-space(marc:subfield[@code='h']) = $old852h])">
            <xsl:apply-templates select="."></xsl:apply-templates>
        </xsl:if>
    </xsl:template>
    
    <!-- Copy template -->
    <xsl:template match="@* | node()">
        <xsl:copy>
            <xsl:apply-templates select="@* | node()"/>
        </xsl:copy>
    </xsl:template>
</xsl:stylesheet>


