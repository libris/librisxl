<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
    <xsl:output encoding="UTF-8" method="xml"/>
    <xsl:template match="data">
        <data>
            <xsl:apply-templates select="categoryitem"/>
        </data>
    </xsl:template>
    
    <xsl:template match="categoryitem">
        <xsl:variable name="id" select="normalize-space(id)"/>
        <categoryitem id="{$id}"><xsl:value-of select="normalize-space(name)"/></categoryitem>
    </xsl:template>
</xsl:stylesheet>