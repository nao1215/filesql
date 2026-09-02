#!/usr/bin/env bash
#
# Writes dialect/testdata/engine_functions_*.txt from the engines themselves.
#
# The tables of names this package refuses were built by sweeping an engine by
# hand, and a sweep is only as complete as the person running it: the one that
# built them walked the scalar functions and never reached the aggregates, so
# five aggregate names went on reaching SQLite for a year. Asking the engine for
# its own catalog is the same question with no one to forget half of it.
#
# Needs docker. Run from the repository root:
#
#	./scripts/dump-engine-functions.sh
#
# Then run the test that reads them:
#
#	go test ./dialect -run TestEveryEngineFunctionHasAnAnswer
#
# GoogleSQL has no counterpart here: BigQuery publishes no catalog of its own
# functions, and the emulator this repository tests against does not either, so
# its table stays hand-written.
set -euo pipefail

cd "$(dirname "$0")/.."
out=dialect/testdata

mysql_image=mysql:8.4
postgres_image=postgres:17-alpine
mysql_container=filesql-function-dump-mysql
postgres_container=filesql-function-dump-postgres

cleanup() {
	docker rm -f "$mysql_container" "$postgres_container" >/dev/null || true
}
trap cleanup EXIT

docker run -d --rm --name "$mysql_container" -e MYSQL_ALLOW_EMPTY_PASSWORD=1 "$mysql_image" >/dev/null
docker run -d --rm --name "$postgres_container" -e POSTGRES_PASSWORD=probe "$postgres_image" >/dev/null

printf 'waiting for the engines'
for _ in $(seq 60); do
	if docker exec "$mysql_container" mysql -uroot -e 'SELECT 1' >/dev/null &&
		docker exec "$postgres_container" pg_isready -U postgres >/dev/null; then
		break
	fi
	printf .
	sleep 2
done
printf '\n'

{
	cat <<'HEADER'
# Every function name MySQL 8.4 defines, from its own help tables.
#
# Regenerate with scripts/dump-engine-functions.sh, which runs the query
# against a MySQL container and writes this file. The list is what the
# engine says it has, so a name added by a later MySQL appears here when
# the file is regenerated rather than when somebody remembers it.
#
# A name here is not necessarily one a caller writes: the data dictionary
# and the performance schema put their own functions in the same tables.
# What matters is that this package has an answer for every one of them.
HEADER
	docker exec "$mysql_container" mysql -uroot -N -B -e "
		SELECT DISTINCT upper(ht.name)
		FROM mysql.help_topic ht
		JOIN mysql.help_category hc ON ht.help_category_id = hc.help_category_id
		WHERE hc.name LIKE '%Function%'
		ORDER BY 1" |
		sed 's/ FUNCTION$//; s/ OPERATOR$//' |
		grep -E '^[A-Z][A-Z0-9_]*$'
} >"$out/engine_functions_mysql.txt"

{
	cat <<'HEADER'
# Every function name PostgreSQL 17 defines that a caller can write, from
# pg_proc.
#
# Regenerate with scripts/dump-engine-functions.sh. The query leaves out
# what implements something else -- an operator, a cast, a type's input
# and output, an aggregate's transitions, an index method's support
# routines -- and what takes or answers a pseudo-type no SQL value can be.
# What is left is what appears in a query somebody writes.
HEADER
	docker exec "$postgres_container" psql -U postgres -tAc "
		WITH implementing AS (
		  SELECT oprcode::oid AS oid FROM pg_operator
		  UNION SELECT amproc::oid FROM pg_amproc
		  UNION SELECT aggtransfn::oid FROM pg_aggregate
		  UNION SELECT aggfinalfn::oid FROM pg_aggregate WHERE aggfinalfn <> 0
		  UNION SELECT aggcombinefn::oid FROM pg_aggregate WHERE aggcombinefn <> 0
		  UNION SELECT aggserialfn::oid FROM pg_aggregate WHERE aggserialfn <> 0
		  UNION SELECT aggdeserialfn::oid FROM pg_aggregate WHERE aggdeserialfn <> 0
		  UNION SELECT aggmtransfn::oid FROM pg_aggregate WHERE aggmtransfn <> 0
		  UNION SELECT aggminvtransfn::oid FROM pg_aggregate WHERE aggminvtransfn <> 0
		  UNION SELECT aggmfinalfn::oid FROM pg_aggregate WHERE aggmfinalfn <> 0
		  UNION SELECT typinput::oid FROM pg_type UNION SELECT typoutput::oid FROM pg_type
		  UNION SELECT typreceive::oid FROM pg_type UNION SELECT typsend::oid FROM pg_type
		  UNION SELECT typmodin::oid FROM pg_type WHERE typmodin <> 0
		  UNION SELECT typmodout::oid FROM pg_type WHERE typmodout <> 0
		  UNION SELECT typanalyze::oid FROM pg_type WHERE typanalyze <> 0
		  UNION SELECT castfunc::oid FROM pg_cast WHERE castfunc <> 0
		  UNION SELECT rngcanonical::oid FROM pg_range WHERE rngcanonical <> 0
		  UNION SELECT rngsubdiff::oid FROM pg_range WHERE rngsubdiff <> 0
		)
		SELECT DISTINCT upper(p.proname)
		FROM pg_proc p
		JOIN pg_namespace n ON p.pronamespace = n.oid
		WHERE n.nspname = 'pg_catalog'
		  AND p.prokind IN ('f','a','w')
		  AND p.proname ~ '^[a-z][a-z0-9_]*\$'
		  AND p.oid NOT IN (SELECT oid FROM implementing)
		  AND p.prorettype::regtype::text NOT IN ('internal','cstring','trigger','event_trigger',
			'language_handler','fdw_handler','index_am_handler','tsm_handler','table_am_handler','void')
		  AND NOT EXISTS (SELECT 1 FROM unnest(p.proargtypes) t WHERE t::regtype::text IN ('internal','cstring'))
		ORDER BY 1" |
		sed 's/^ *//' |
		grep -v '^$'
} >"$out/engine_functions_postgresql.txt"

wc -l "$out"/engine_functions_*.txt
