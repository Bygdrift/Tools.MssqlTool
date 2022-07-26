using Bygdrift.Tools.CsvTool;
using Bygdrift.Tools.MssqlTool.Models;
using RepoDb;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("MssqlToolTests")]
namespace Bygdrift.Tools.MssqlTool.Helpers
{
    internal class PrepareTableForCsv
    {
        private readonly Mssql mssql;
        private readonly string tableName;

        internal PrepareTableForCsv(Mssql mssql, Csv csv, string tableName, string primaryKey)
        {
            this.mssql = mssql;
            this.tableName = tableName;
            var sql = "";

            if (string.IsNullOrEmpty(tableName))
                throw new ArgumentNullException(nameof(tableName), "Table name is null. It has to be set.");

            var colTypes = GetColTypes(csv, primaryKey);

            if (colTypes == null || !colTypes.Any())
                return;

            if (colTypes.All(o => !o.IsSetForSql))
                sql = CreateTableAndColumns(colTypes);
            else
                sql = UpdateColumns(colTypes);

            if (!string.IsNullOrEmpty(sql))
            {
                try
                {
                    mssql.Connection.ExecuteNonQuery(sql);
                }
                catch (Exception e)
                {
                    mssql.Log.Add(LogTool.Models.LogType.Critical, e, "Error in db load: {Message}. Commands: {Commands}", e.Message, sql);
                    throw new Exception($"Error in db load: {e.Message}. Commands: {sql}", e);
                }
                mssql.FlushRepoDb();
            }
        }

        private List<ColumnType> GetColTypes(Csv csv, string csvPrimaryKey)
        {
            var colTypes = mssql.GetColumnTypes(tableName).ToList();

            foreach (var sqlColType in colTypes)
                if (csv.TryGetColId(sqlColType.Name, out int csvColId, false))  //Not caseSensitive because SQL are not
                {
                    var csvIsPrimaryKey = csvPrimaryKey != null && csvPrimaryKey.Equals(sqlColType.Name);
                    sqlColType.AddCsv(csv.ColTypes[csvColId], csv.ColMaxLengths[csvColId], csvIsPrimaryKey);
                }

            var notInSqlHeaders = csv.Headers.Values.Except(colTypes.Select(o => o.Name));
            foreach (var name in notInSqlHeaders)
            {
                if (csv.TryGetColId(name, out int csvColId, false) && csv.ColTypes.Any() && csv.ColMaxLengths.Any())  //Not caseSensitive because SQL are not
                {
                    var isPrimaryKey = csvPrimaryKey != null && csvPrimaryKey.Equals(name);
                    colTypes.Add(new ColumnType(name).AddCsv(csv.ColTypes[csvColId], csv.ColMaxLengths[csvColId], isPrimaryKey));
                }
            }
            return colTypes;
        }

        private string CreateTableAndColumns(List<ColumnType> colTypes)
        {
            CreateSchemaIfNotExists();
            var cols = "";
            foreach (var colType in colTypes)
            {
                colType.TryGetUpdatedChangedType(out string typeExpression);
                cols += $"[{colType.Name}] {typeExpression} " + (colType.IsPrimaryKeyCsv ? "NOT NULL PRIMARY KEY" : "NULL") + ",\n";
            }
            return $"CREATE TABLE [{mssql.SchemaName}].[{tableName}](\n{cols})";
        }

        private string UpdateColumns(List<ColumnType> colTypes)
        {
            var sql = "";
            foreach (var colType in colTypes)
            {
                if (colType.IsPrimaryKeyCsv && !colType.IsPrimaryKeySql)  //PrimaryKey added
                {
                    if (colType.IsNullableSql)  //A normal column has been added and must be upgraded to a primary key:
                    {
                        colType.TryGetUpdatedChangedType(out string typeExpression);
                        mssql.ExecuteNonQuery($"ALTER TABLE [{mssql.SchemaName}].[{tableName}] ALTER COLUMN [{colType.Name}] {typeExpression} NOT NULL;");
                    }

                    sql += $"ALTER TABLE [{mssql.SchemaName}].[{tableName}] ADD CONSTRAINT [{CreateConstraintName(colType.Name)}] PRIMARY KEY ([{colType.Name}]);\n";
                }
                else if (!colType.IsPrimaryKeyCsv && colType.IsPrimaryKeySql)  //PrimaryKey removed. Has to be run before ADD and thats why it is not added to sql+=...
                {
                    mssql.ExecuteNonQuery($"ALTER TABLE [{mssql.SchemaName}].[{tableName}] DROP CONSTRAINT {colType.ConstraintSql};");
                }
                else if (colType.TryGetUpdatedChangedType(out string typeExpression))  //Update column
                {
                    if (colType.IsPrimaryKeyCsv && colType.IsPrimaryKeySql)  //PrimaryKey updated
                    {
                        sql += "DECLARE @constraint varchar(128);\n";
                        sql += $"SELECT @constraint = CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = '{mssql.SchemaName}' AND TABLE_NAME = '{tableName}';\n";
                        sql += $"if (@constraint) IS NOT NULL EXEC('ALTER TABLE [{mssql.SchemaName}].[{tableName}] DROP CONSTRAINT ' + @constraint);\n";
                        sql += $"ALTER TABLE [{mssql.SchemaName}].[{tableName}] ALTER COLUMN [{colType.Name}] { typeExpression} NOT NULL;\n";
                        sql += $"ALTER TABLE [{mssql.SchemaName}].[{tableName}] ADD CONSTRAINT [{CreateConstraintName(colType.Name)}] PRIMARY KEY ([{colType.Name}]);\n";
                    }
                    else  //Normal colum updated
                        sql += $"ALTER TABLE [{mssql.SchemaName}].[{tableName}] ALTER COLUMN [{colType.Name}] {typeExpression};\n";
                }
                else if (!colType.IsSetForSql)  //Normal column added
                    sql += $"ALTER TABLE [{mssql.SchemaName}].[{tableName}] ADD [{colType.Name}] {typeExpression};\n";
            }
            return sql;
        }

        internal string CreateConstraintName(string columnName)
        {
            return string.Concat("PK__", columnName, Guid.NewGuid().ToString("N").ToUpper().AsSpan(0, 16));
        }

        private void CreateSchemaIfNotExists()
        {
            mssql.Connection.ExecuteNonQuery($"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{mssql.SchemaName}') BEGIN EXEC('CREATE SCHEMA {mssql.SchemaName}') END");
        }
    }
}
