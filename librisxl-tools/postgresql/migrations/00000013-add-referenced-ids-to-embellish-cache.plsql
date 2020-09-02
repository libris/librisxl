BEGIN;

DO $$DECLARE
   -- THESE MUST BE CHANGED WHEN YOU COPY THE SCRIPT!
   
   -- The version you expect the database to have _before_ the migration
   old_version numeric := 12;
   -- The version the database should have _after_ the migration
   new_version numeric := 13;

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
   DROP TABLE IF EXISTS lddb__export_embellished;
   DROP TABLE IF EXISTS lddb__embellished;
   CREATE TABLE lddb__embellished (
      id text NOT NULL UNIQUE primary key,
      data jsonb NOT NULL,
      ids text[] NOT NULL
   );

   CREATE INDEX ON lddb__embellished USING gin (ids);

END$$;

COMMIT;
