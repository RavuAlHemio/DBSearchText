# DBSearchText

Searches through all values of all textual columns of all tables (of all schemas) (of all databases) available through a
database engine for a substring and outputs all matches found.

Useful for reverse engineering database structures (input known data using the official client, then look for it using
this tool).

## Supported database engines

* Microsoft SQL Server
* MySQL and MariaDB
* Oracle (one service at a time)
* PostgreSQL (one database at a time)
* SQLite
