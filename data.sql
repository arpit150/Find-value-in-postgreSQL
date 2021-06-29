select arpit_test.base_status_test('arpit_test','match','match_1','report')

CREATE OR REPLACE FUNCTION arpit_test.base_status_test(
	schema_name character varying,
	table_name character varying,
	table_name_1 character varying,
	report character varying )
    RETURNS integer
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
DECLARE 
returnstatus varchar;
DECLARE 
f1 text; f2 text;
t1 text; t2 text;
DECLARE SQLQuery varchar;
DECLARE error_tab_name varchar;
DECLARE v_cnt  varchar;
DECLARE ROW_COUNT  varchar;

BEGIN

returnstatus =1 ;
error_tab_name = 'arpit_test.error';

SQLQuery = 'Drop Table If Exists '||schema_name||'."'||report||'"';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery ='create table '||schema_name||'."'||report||'" as
select id,
        case 
		    when t.id is null then ''deleted in  table''
			when t12.id is null then ''added in table''
			else
			     ''data updated''
		end as REMARKS,
			skeys(hstore(t)-hstore(t12)) "base_table_",
			svals(hstore(t)-hstore(t12)) "base_table",
		case 
			 when t.id is null then 1
			 when t12.id is null then 2
				else
					  3
		end as base_table_status,
				
		skeys(hstore(t12)-hstore(t)) "base_table__1",
		svals(hstore(t12)-hstore(t))  "base_table_1",
		0 as base_table_1_status
			
from '||schema_name||'."'||table_name||'" t
                       full outer join  '||schema_name||'."'||table_name_1||'" t12 using(id)
where t is distinct from t12';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;
	
return returnstatus;
EXCEPTION
	WHEN OTHERS THEN
	GET STACKED DIAGNOSTICS 
		f1=MESSAGE_TEXT,
		f2=PG_EXCEPTION_CONTEXT; 
		RAISE info 'error caught:%',f1;
		RAISE info 'error caught:%',f2;
		SQLQuery = FORMAT('INSERT INTO %1$s (table_name,table_schema,message,context) Values(''%2$s'',''%3$s'',''%4$s'',''%5$s'')',error_tab_name,table_name,schema_name,f1,f2);
		RAISE INFO 'error inserted ------>%',SQLQuery;
		EXECUTE SQLQuery;
		return returnstatus;
END 
$BODY$;

