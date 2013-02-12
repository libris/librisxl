var fs = require('fs');
var marcjson = require('./lib/marcjson');

function loadJson(path) {
  var data = fs.readFileSync(path, 'utf-8');
  return JSON.parse(data);
}

if (!process.argv[2]) {
  console.log("USAGE: map-marcjson.js MARCMAP_FILE bib MARC_JSON_FILE [OVERLAY_FILE]")
  process.exit();
}
var marcMapPath = process.argv[2];
var recordType = process.argv[3];
var marcStructPath = process.argv[4];
var op = process.argv[5];

var marcmap = loadJson(marcMapPath), submap = marcmap
if (recordType)
  submap = marcmap[recordType];
var struct = loadJson(marcStructPath);

var out;
if (op === '-n') {
  out = marcjson.rawToNamed(submap, struct);
} else if (op !== undefined) {
  var overlay = loadJson(op);
  out = marcjson.createEntityGroups(submap, overlay, struct);
} else {
  marcjson.expandFixedFields(submap, struct);
  out = struct;
}

console.log(JSON.stringify(out, null, 2));

