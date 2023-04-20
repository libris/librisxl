BEGIN;

DO $$DECLARE
   -- THESE MUST BE CHANGED WHEN YOU COPY THE SCRIPT!

   -- The version you expect the database to have _before_ the migration
   old_version numeric := 20;
   -- The version the database should have _after_ the migration
   new_version numeric := 21;

   -- hands off
   existing_version numeric;

BEGIN

   -- Check existing version
SELECT version from lddb__schema INTO existing_version;
IF ( existing_version <> old_version) THEN
      RAISE EXCEPTION 'ASKED TO MIGRATE FROM INCORRECT EXISTING VERSION!';
ROLLBACK;
END IF;
UPDATE lddb__schema SET version = new_version;

-- ACTUAL SCHEMA CHANGES HERE:

ALTER TABLE lddb__versions ADD UNIQUE (pk);
CREATE TABLE IF NOT EXISTS lddb__notices (
    pk SERIAL PRIMARY KEY,
    versionid INTEGER,
    userid TEXT,
    handled BOOLEAN DEFAULT FALSE,
    created timestamp with time zone DEFAULT now() NOT NULL,
    
    CONSTRAINT version_fk FOREIGN KEY (versionid) REFERENCES lddb__versions(pk) ON DELETE CASCADE,
    CONSTRAINT user_fk FOREIGN KEY (userid) REFERENCES lddb__user_data(id) ON DELETE CASCADE
);
CREATE INDEX idx_notices ON lddb__notices USING BTREE (userid);

END$$;

COMMIT;
