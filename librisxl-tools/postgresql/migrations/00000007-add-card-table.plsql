BEGIN;

DO $$DECLARE
   -- THESE MUST BE CHANGED WHEN YOU COPY THE SCRIPT!
   
   -- The version you expect the database to have _before_ the migration
   old_version numeric := 6;
   -- The version the database should have _after_ the migration
   new_version numeric := 7;

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
   CREATE TABLE lddb__cards (
      id       text  PRIMARY KEY,
      data     jsonb NOT NULL,
      checksum text  NOT NULL,
      updated  timestamp with time zone NOT NULL,
      changed  timestamp with time zone NOT NULL
   );

   TRUNCATE TABLE lddb__embellished;

END$$;

COMMIT;
