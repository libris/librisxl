CREATE INDEX idx_lddb_alive ON lddb (id) WHERE deleted IS NOT true;
CREATE INDEX idx_lddb_modified ON lddb (modified);
CREATE INDEX idx_lddb_manifest ON lddb USING GIN (manifest jsonb_path_ops);
CREATE INDEX idx_lddb_quoted ON lddb USING GIN (quoted jsonb_path_ops);

CREATE INDEX idx_lddb_graph ON lddb USING GIN ((data->'@graph') jsonb_path_ops);

CREATE INDEX idx_lddb_collection ON lddb ((manifest->>'collection'));
CREATE INDEX idx_lddb_alt_ids ON lddb USING GIN ((manifest->'identifiers') jsonb_path_ops);

CREATE INDEX idx_lddb__identifiers_id ON lddb__identifiers (id);

CREATE INDEX idx_lddb__versions_id ON lddb__versions (id);
CREATE INDEX idx_lddb__versions_modified ON lddb__versions (modified);
CREATE INDEX idx_lddb__versions_checksum ON lddb__versions (checksum);
CREATE INDEX idx_lddb__versions_manifest ON lddb__versions USING GIN (manifest jsonb_path_ops);
CREATE INDEX idx_lddb__versions_collection ON lddb__versions ((manifest->>'collection'));
