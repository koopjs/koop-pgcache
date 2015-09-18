DO
$$
Declare
	rec record;
BEGIN
	FOR rec IN
		SELECT table_name,column_name
		FROM information_schema.columns
		WHERE data_type='json'
	LOOP
		EXECUTE
		'ALTER TABLE '||quote_ident(rec.table_name)||
			' ALTER COLUMN '||quote_ident(rec.column_name)||
			' SET DATA TYPE jsonb '||
			' USING '||quote_ident(rec.column_name)||'::jsonb;';
	END LOOP;
END
$$;

