/* 

!!! Must run after lxl-1556-1

// find all bib instanceOf.marc:version properties (approx 1853)


// auth?
// instanceOf.subject olänkade verk

*/

// marc:version -> version
OLD_PROP = 'marc:version'
NEW_PROP = 'version'

def where = """
  collection = 'bib' AND
  (
     data#>>'{@graph,1,instanceOf,marc:version}' OR
     data#>>'{@graph,1,instanceOf,expressionOf,marc:version}' OR
     data#>>'{@graph,1,instanceOf,hasPart,marc:version}' OR
     data#>>'{@graph,1,instanceOf,relationship,entity,marc:version}'
     data#>>'{@graph,1,instanceOf,subject,marc:version}' IS NOT NULL
  ) 
  AND deleted = false
"""

/*
Also auth Works has version and hasVariant.version

def where = """
  collection = 'auth' AND
  (
    data#>>'{@graph,1,marc:version}' OR 
    data#>>'{@graph,1,hasVariant,marc:version}'
    ) IS NOT NULL
  AND deleted = false
"""
*/
