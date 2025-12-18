# Apache Ignite EF Core Provider Development Notes

## Where to Look for Things
* Migrations SQL generation: `IgniteMigrationsSqlGenerator`
* Literal value formatting: `*TypeMapping` classes, e.g. `IgniteDateTimeTypeMapping`

## Known Issues
* Auto-generated columns
  * Ignite only supports random UUIDs: `create table t1 (id uuid default rand_uuid primary key, val int)`
  * No way to get back the auto-generated value for the `INSERT` statement (which is needed by EF Core)
* Tables without `PRIMARY KEY` are not supported

## To Be Implemented
* Fix all compiler warnings and enable all analyzers
* Add test for all Ignite-specific column types
* Bring in more specification tests (https://issues.apache.org/jira/browse/IGNITE-22134)
