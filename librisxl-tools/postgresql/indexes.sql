CREATE INDEX idx_lddb_alive ON lddb (id) WHERE deleted IS NOT true;
CREATE INDEX idx_lddb_modified ON lddb (modified);
CREATE INDEX idx_lddb_dep_min_modified ON lddb (depMinModified);
CREATE INDEX idx_lddb_dep_max_modified ON lddb (depMaxModified);
CREATE INDEX idx_lddb_graph ON lddb USING GIN ((data->'@graph') jsonb_path_ops);
CREATE INDEX idx_lddb_holding_for on lddb ((data#>>'{@graph,1,itemOf,@id}'));
CREATE INDEX idx_lddb_held_by on lddb ((data#>>'{@graph,1,heldBy,@id}'));
CREATE INDEX idx_lddb_systemnumber on lddb using gin ((data#>'{@graph,0,systemNumber}'));
CREATE INDEX idx_lddb_thing_identifiers on lddb using gin ((data#>'{@graph,1,identifier}'));

CREATE INDEX idx_lddb__identifiers_id ON lddb__identifiers (id);
CREATE INDEX idx_lddb__identifiers_iri ON lddb__identifiers (iri);

CREATE INDEX idx_lddb__dependencies_id ON lddb__dependencies (id);
CREATE INDEX idx_lddb__dependencies_depends_on ON lddb__dependencies (dependsOnId);

CREATE INDEX idx_lddb__versions_id ON lddb__versions (id);
CREATE INDEX idx_lddb__versions_modified ON lddb__versions (modified);
CREATE INDEX idx_lddb__versions_checksum ON lddb__versions (checksum);
