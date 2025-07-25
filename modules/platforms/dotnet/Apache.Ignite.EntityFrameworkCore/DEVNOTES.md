# Apache Ignite EF Core Provider Development Notes

## Known Issues
* Auto-generated columns
  * Ignite only supports random UUIDs: `create table t1 (id uuid default rand_uuid primary key, val int)`
  * No way to get back the auto-generated value for the `INSERT` statement (which is needed by EF Core)
* Tables without `PRIMARY KEY` are not supported

## To Be Implemented
* Parse connection string into `IgniteClientConfiguration` - is there a standard parser for this?
