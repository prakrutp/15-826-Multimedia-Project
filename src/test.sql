CREATE FUNCTION add(integer, integer) RETURNS integer AS $$
    SELECT $1 + $2;
 $$ LANGUAGE SQL;

SELECT add(1, 2) AS answer;


CREATE FUNCTION clean_darpa() RETURNS void AS $$
DELETE FROM darpa_agg
	WHERE count < 0;
$$ LANGUAGE SQL;
SELECT clean_darpa();

CREATE OR REPLACE FUNCTION fib (fib_for integer) RETURNS integer AS $$
	BEGIN
		IF fib_for < 2 THEN
		RETURN fib_for;
	END IF;
		RETURN fib(fib_for - 2) + fib(fib_for - 1);
END;
$$ LANGUAGE plpgsql;

SELECT fib(8);


	