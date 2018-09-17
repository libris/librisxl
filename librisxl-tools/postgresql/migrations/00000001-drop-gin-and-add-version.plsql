BEGIN;

DO $$DECLARE
   -- THESE MUST BE CHANGED WHEN YOU COPY THE SCRIPT!
   
   -- The version you expect the database to have _before_ the migration
   old_version numeric := 0;
   -- The version the database should have _after_ the migration
   new_version numeric := 1;

   -- hands off
   existing_version numeric;

BEGIN

   -- Set up database version tracking (only this first time, DON'T COPY this so subsequent scripts)
   CREATE TABLE lddb__schema (
      version numeric
   );
   INSERT INTO lddb__schema values (0);
   --

   -- Check existing version
   SELECT version from lddb__schema INTO existing_version;
   IF ( existing_version <> old_version) THEN
      RAISE EXCEPTION 'ASKED TO MIGRATE FROM INCORRECT EXISTING VERSION!';
      ROLLBACK;
   END IF;
   UPDATE lddb__schema SET version = new_version;


   -- ACTUAL SCHEMA CHANGES HERE:
   DROP INDEX idx_lddb_graph;

END$$;

COMMIT;
