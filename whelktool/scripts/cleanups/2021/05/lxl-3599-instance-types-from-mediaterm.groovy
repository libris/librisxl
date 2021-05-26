String where = """
    collection = 'bib'
    AND data#>>'{@graph,1,@type}' = 'Instance'
    AND data#>'{@graph,1,marc:mediaTerm}' IS NOT NULL
"""

selectBySqlWhere(where){ data ->
    Map instance = data.graph[1]
    String workType = instance.instanceOf?."@type"
    String mediaTerm = instance."marc:mediaTerm"

    boolean modified

    if (mediaTerm =~ /(?i)musiktryck/ && workType ==~ /NotatedMusic|Text/) {
        instance."@type" = "Print"
        modified = true
    }
    else if (mediaTerm ==~ /(?i).?ljudupptagning.?(\(CD\))?/ && workType ==~ /Audio|Music/) {
        instance."@type" = "SoundRecording"
        modified = true
    }
    else if (mediaTerm ==~ /(?i)(ljud|tal)bok ?\(?CD(-R)?\)?/ && workType == "Audio") {
        instance."@type" = "SoundRecording"
        modified = true
    }
    else if (mediaTerm =~ /(?i)videoupptagning/ && workType == "MovingImage") {
        instance."@type" = "VideoRecording"
        modified = true
    }
    else if (mediaTerm ==~ /(?i).?ele[ck]troni(c|sk) reso?ur(s|ce).?/ && workType ==~ /Text|Multimedia/) {
        instance."@type" = "Electronic"
        modified = true
    }
    else if (mediaTerm =~ /(?i)text[eo] impr/ && workType == "Text") {
        instance."@type" = "Print"
        modified = true
    }
    else if (mediaTerm == "Datafil" && workType == "Multimedia") {
        instance."@type" = "Electronic"
        modified = true
    }

    if (modified)
        data.scheduleSave()
}
