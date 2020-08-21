BEGIN;

DO $$DECLARE
   -- THESE MUST BE CHANGED WHEN YOU COPY THE SCRIPT!
   
   -- The version you expect the database to have _before_ the migration
   old_version numeric := 11;
   -- The version the database should have _after_ the migration
   new_version numeric := 12;

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
   CREATE OR REPLACE FUNCTION lddb__notify_card()
       RETURNS trigger AS $$
   DECLARE
   BEGIN
       PERFORM pg_notify('lddb__cards_changed', OLD.id);
       RETURN NEW;
   END;
   $$ LANGUAGE plpgsql;

   CREATE TRIGGER lddb__notify_card
       AFTER UPDATE OR DELETE ON lddb__cards
       FOR EACH ROW
       EXECUTE PROCEDURE lddb__notify_card();

END$$;

COMMIT;
