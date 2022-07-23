# Bygdrift.Tools.Mssql

Make a CSV with [Bygdrift.Tools.Csv](https://github.com/Bygdrift/Tools.CsvTool) and use this Mssql-tool to insert or merge data into a table in a Microsoft SQL database.

This tool creates or updates columns in database tables. It checks what data type a column has in csv and looks if the data type should be updated in the database.

It's also easy to change primary key.

It will soon be available at Nuget.

## Get started

```c#
Csv csv = new Csv("Id, Name").AddRows("A, Anders", "B, Bo");

```