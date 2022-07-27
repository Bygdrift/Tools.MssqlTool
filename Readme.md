# Bygdrift.Tools.MssqlTool

Make a CSV with [Bygdrift.Tools.Csv](https://github.com/Bygdrift/Tools.CsvTool) and use this Mssql-tool to insert or merge data into a table in a Microsoft SQL database.

This tool creates or updates columns in database tables. It checks what data type a column has in csv and looks if the data type should be updated in the database.

It's also easy to change primary key.

It will soon be available at Nuget.

## Get started

```c#
//Create some data:
Csv csv = new Csv("Id, Name").AddRow(1, "Anders").AddRow(2, "Bo");

//Open the connection to the database and only work with tables that uses the schema 'schemaName':
Mssql mssql = new Mssql("connectionString", "schemaName", new Bygdrift.Tools.LogTool.Log());

//Merge data into a table called TableName. If it doesn't exists, it will be created and have the schema 'schemaName':
mssql.MergeCsv(csv, "TableName", "Id", false);

//Update the name in first row:
mssql.MergeCsv(new Csv("Id, Name").AddRow(1, "Erik"), "TableName", "Id", false);

//Add a column named 'Age':
mssql.MergeCsv(new Csv("Id, Name, Age").AddRow(1, "Erik", 23), "TableName", "Id", false);
```

## Merge vs Insert

If there is no primary key, it's easy, just to input data with InsertCsv, while MergeCsv will ad a primaryKey:

```c#
//This inserts two rows
mssql.InsertCsv(new Csv("Id, Name").AddRow(1, "Anders"), "TableName", false, false);
mssql.InsertCsv(new Csv("Id, Name").AddRow(1, "Bo"), "TableName", false, false);

//While this only inserts one row that then gets updated:
mssql.MergeCsv(new Csv("Id, Name").AddRow(1, "Anders"), "TableName", "Id", false);
mssql.MergeCsv(new Csv("Id, Name").AddRow(1, "Bo"), "TableName", "Id", false);
```

## Other commands
```c#
mssql.DeleteTable(string tableName)  //Delete the table if it's within the schemaName, defined in Mssql
mssql.DeleteAllTables()  //Deletes all tables that are within the schemaName, defined in Mssql
mssql.DeleteOldRows(string tableName, string columnName, DateTime expirationTime)  //Removes rows that are older than a given expiration data
mssql.ExecuteNonQuery(string sql)  //Excecute a SQL
mssql.GetAsCsv(string tableName)  //Returns all data as csv
```

