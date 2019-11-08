BEGIN;

DO $$DECLARE
   -- THESE MUST BE CHANGED WHEN YOU COPY THE SCRIPT!
   
   -- The version you expect the database to have _before_ the migration
   old_version numeric := 3;
   -- The version the database should have _after_ the migration
   new_version numeric := 4;

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
   DROP INDEX idx_lddb_alive; -- Was never used.
   DROP INDEX idx_lddb__versions_checksum; -- Was intended for not-inserting-duplicate-versions. Never used.
   DROP INDEX idx_lddb_dep_min_modified; -- Was intended for corner case exports via OAIPMH. Was never relevant and is no longer used at all.
   DROP INDEX idx_lddb_systemnumber; -- System numbers moved house (into identifiedBy), thus this is no longer of any use.

END$$;

COMMIT;
