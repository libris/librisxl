BEGIN;

DO $$DECLARE
   -- THESE MUST BE CHANGED WHEN YOU COPY THE SCRIPT!
   
   -- The version you expect the database to have _before_ the migration
   old_version numeric := 23;
   -- The version the database should have _after_ the migration
   new_version numeric := 24;

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


   -- The actual diffs (eventually to replace the versions table).
   CREATE TABLE IF NOT EXISTS lddb__history (
       id TEXT PRIMARY KEY, -- refers to id in lddb
       history JSONB
   );

   -- To answer the question every export needs: "Which records changed between time X and Y".
   -- The lack of a primay key is intentional.
   CREATE TABLE IF NOT EXISTS lddb__change_times (
       id TEXT, -- refers to id in lddb
       time TIMESTAMP WITH TIME ZONE, 
       loud BOOLEAN
   );
   CREATE INDEX IF NOT EXISTS idx_lddb__change_times_time ON lddb__change_times ((time));
   
   
END$$;

COMMIT;
