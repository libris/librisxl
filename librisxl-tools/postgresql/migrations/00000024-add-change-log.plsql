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

   CREATE TABLE IF NOT EXISTS lddb__change_log (
    changenumber BIGSERIAL PRIMARY KEY,
    id text NOT NULL,
    loud BOOLEAN NOT NULL,
    time timestamp with time zone DEFAULT now() NOT NULL,
    resulting_record_version INTEGER NOT NULL
   );

   CREATE INDEX IF NOT EXISTS idx_lddb__change_log_time ON lddb__change_log (time);	

END$$;

COMMIT;




