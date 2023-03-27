CREATE OR REPLACE PROCEDURE ews.changevarcoltype(
	schemaname character varying,
	tablename character varying,
	skiplist character varying[])
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
	list ALIAS FOR $3;
    col RECORD;
    maxResult INT = 0;
	colType varchar;
	checkNull boolean;
BEGIN
    RAISE NOTICE 'Start to shrink table %.% ...', schemaname, tableName;
	RAISE NOTICE 'Skip Column List: %', list;
	FOR col IN SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = tableName
    LOOP
		If not lower(col.column_name) = any(lower(list::varchar)::varchar[]) then 
			EXECUTE 'select exists(select "' ||  col.column_name || '" from '|| schemaname ||'."' || tableName || '" where "' ||  col.column_name || '" is not null)' into checkNull;
			if checkNull is true then
				select data_type FROM information_schema.columns WHERE table_name = tableName and column_name = col.column_name into colType;
				if lower(colType) = 'character varying' then
					EXECUTE 'ALTER TABLE ' || schemaname || '."' || tableName || '" ALTER COLUMN "' ||  col.column_name || '" TYPE VARCHAR(255)';
					EXECUTE 'SELECT MAX(LENGTH("' || col.column_name || '")) FROM '|| schemaname ||'."' || tableName || '"' into maxResult;
					RAISE NOTICE 'Column "%", Max Result Length: %', col.column_name, maxResult;
					if maxResult = 0 then
						maxResult = maxResult +  1;
					end if;
					EXECUTE 'ALTER TABLE ' || schemaname || '."' || tableName || '" ALTER COLUMN "' ||  col.column_name || '" TYPE VARCHAR(' || maxResult || ')';
				else
					RAISE NOTICE 'Skip Column not varchar type: %', col.column_name;
				end if;
			else
				RAISE NOTICE 'Skip Null Column: %', col.column_name;
			end if;
		else 
			RAISE NOTICE 'Skip Column Name: %', col.column_name;
		end if;
	END LOOP;

    RAISE NOTICE 'Done for shrinking table %.% .', schemaname, tableName;
END;
$BODY$;
