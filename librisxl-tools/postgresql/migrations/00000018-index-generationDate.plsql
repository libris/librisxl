BEGIN;

DO $$DECLARE
   -- THESE MUST BE CHANGED WHEN YOU COPY THE SCRIPT!
   
   -- The version you expect the database to have _before_ the migration
   old_version numeric := 17;
   -- The version the database should have _after_ the migration
   new_version numeric := 18;

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

   CREATE OR REPLACE FUNCTION totstz(text)
   RETURNS timestamptz AS
   'SELECT $1::timestamptz'
   LANGUAGE sql IMMUTABLE;

   CREATE INDEX idx_lddb_generation_date ON lddb (totstz(data#>>'{@graph,0,generationDate}'));
   
END$$;

COMMIT;
