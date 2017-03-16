CREATE OR REPLACE FUNCTION mass(_tbl regclass, OUT result integer) AS $func$ 
BEGIN 
EXECUTE format('SELECT sum(count) FROM %s', _tbl) INTO result; 
END $func$ 
LANGUAGE plpgsql;
DECLARE @M_R INT
SELECT @M_R = SELECT mass('darpa_agg')
DECLARE @M_B INT
create table tmp as (select * from darpa_agg limit 10);
SELECT @M_B = select mass('tmp');
drop table tmp;
