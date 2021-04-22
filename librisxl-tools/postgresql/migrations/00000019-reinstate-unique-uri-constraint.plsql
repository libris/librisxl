BEGIN;

DO $$DECLARE
   -- THESE MUST BE CHANGED WHEN YOU COPY THE SCRIPT!
   
   -- The version you expect the database to have _before_ the migration
   old_version numeric := 18;
   -- The version the database should have _after_ the migration
   new_version numeric := 19;

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

   -- At the time of writing, there is an IRI conflict on 'https://id.kb.se/marc/SerialsFrequencyType-z'. This has now been fixed in the definitions repo, but the bad data is still live. As a failsafe, if this migrations is run _before_ the definitions are reloaded, delete the conflicting line, to make the constraint possible to add.
   DELETE FROM lddb__identifiers WHERE id = 'kxq3td5nm2lnmx23' AND iri = 'https://id.kb.se/marc/SerialsFrequencyType-z';
   
   ALTER TABLE lddb__identifiers ADD CONSTRAINT unique_uri UNIQUE (iri);
   
END$$;

COMMIT;
