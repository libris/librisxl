// Open update file
def updateFile = new File(scriptDir, 'updatefile.jsonl')

// Open create file
def createFile = new File(scriptDir, 'updatefile.jsonl')

// Set up logging
def report = getReportWriter("INFO.tsv")

//// Let's start by updating matched Libris records ////
// Read update file as json lines
def updateLibris = updateFile.readLines()

// For each line
//  Get Libris ID from line
//  Fetch Libris record from Libris
//  Add fields and values from SHB to Libris record
//      bibliography:SHB
//      SAO headings (don't create duplicates)
//      SAB headings (don't create duplicates)
//      SHB as metadata source
//  Save updated record to Libris


//// Now let's create new Libris records ////
// Read create file as json lines
def createLibris = createFile.readLines()

// For each line
//  Save new record to Libris

// The end

/////////////