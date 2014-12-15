var fs = require('fs');
var marcfrbr = require('./lib/marcfrbr')

var marcMapPath = "../whelk-extensions/src/main/resources/marcmap.json"
var marcStructPath = process.argv[2]

var submap = require(marcMapPath)['bib']
var struct = JSON.parse(fs.readFileSync(marcStructPath, 'utf-8'))

var out = marcfrbr.toFrbrStruct(submap, struct)

console.log(JSON.stringify(out, null, 2))
