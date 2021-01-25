DO $$
BEGIN
    EXECUTE (
        SELECT 'DROP TABLE ' || string_agg(tablename, ', ')
        FROM   pg_tables
        WHERE  tablename LIKE 'lddb__' || '%'
        AND    schemaname = current_schema()
    );

    DROP TABLE lddb;
END$$;
