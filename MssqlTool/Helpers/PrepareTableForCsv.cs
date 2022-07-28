using Bygdrift.Tools.CsvTool;
using Bygdrift.Tools.LogTool.Models;
using Bygdrift.Tools.MssqlTool.Models;
using RepoDb;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("MssqlToolTests")]
namespace Bygdrift.Tools.MssqlTool.Helpers
{
    /// <summary>
    /// Add or update a table in a DB so columns can contain the content from incomming csv
    /// </summary>
    internal class PrepareTableForCsv
    {
        private readonly Mssql mssql;
        private readonly string tableName;

        internal PrepareTableForCsv(Mssql mssql, Csv csv, string tableName, string primaryKey)
        {
            if (string.IsNullOrEmpty(tableName))
                throw new ArgumentNullException(nameof(tableName), "Table name is null. It has to be set.");

            this.tableName = tableName;
            this.mssql = mssql;
            ColumnTypes = mssql.GetColumnTypes(tableName).ToList();
            ColumnType.AddCsv(csv, primaryKey, ColumnTypes);

            if (!ColumnTypes.Any())
                return;
            else if (ColumnTypes.All(o => !o.IsSetForSql))
                ExecuteSqls(CreateTableAndColumns(ColumnTypes));
            else
                ExecuteSqls(UpdateColumns(ColumnTypes));
        }
        public List<ColumnType> ColumnTypes { get; set; }

        private string CreateTableAndColumns(List<ColumnType> columns)
        {
            CreateSchemaIfNotExists();
            var cols = "";
            foreach (var colType in columns)
                cols += $"[{colType.Name}] {colType.TypeExpression} " + (colType.IsPrimaryKeyCsv ? "NOT NULL PRIMARY KEY" : "NULL") + ",\n";

            return $"CREATE TABLE [{mssql.SchemaName}].[{tableName}](\n{cols})";
        }

        private string[] UpdateColumns(List<ColumnType> columns)
        {
            var sql = "";
            var sqls = new List<string>();
            foreach (var colType in columns.OrderBy(o => o.IsPrimaryKeyCsv).ThenBy(o => o.IsPrimaryKeySql))
            {
                if (colType.ChangedPrimaryKey == ChangePrimaryKey.Add)  //PrimaryKey added
                    AddPrimaryKey(colType, sqls, ref sql);
                else if (colType.ChangedPrimaryKey == ChangePrimaryKey.Remove)  //PrimaryKey removed. Has to be run before ADD and thats why it is not added to sql+=...
                {
                    AddSql(sqls, ref sql);
                    sqls.Add($"ALTER TABLE [{mssql.SchemaName}].[{tableName}] DROP CONSTRAINT {colType.ConstraintSql};");
                }
                else if (colType.Change == Change.Add)  //Normal column added
                    sql += $"ALTER TABLE [{mssql.SchemaName}].[{tableName}] ADD [{colType.Name}] {colType.TypeExpression};\n";

                else if (colType.Change == Change.Upgrade || colType.Change == Change.Downgrade || colType.Change == Change.Equal)  //Update column
                    UpdateColumn(colType, ref sql);

            }
            AddSql(sqls, ref sql);
            return sqls.ToArray();
        }

        private void AddPrimaryKey(ColumnType colType, List<string> sqls, ref string sql)
        {
            if (colType.IsNullableSql)  //A normal column has been added and must be upgraded to a primary key:
            {
                AddSql(sqls, ref sql);
                sqls.Add($"ALTER TABLE [{mssql.SchemaName}].[{tableName}] ALTER COLUMN [{colType.Name}] {colType.TypeExpression} NOT NULL;");
            }

            sql += $"ALTER TABLE [{mssql.SchemaName}].[{tableName}] ADD CONSTRAINT [{CreateConstraintName(colType.Name)}] PRIMARY KEY ([{colType.Name}]);\n";
        }

        private void UpdateColumn(ColumnType colType, ref string sql)
        {
            if (colType.IsPrimaryKeyCsv && colType.IsPrimaryKeySql)  //PrimaryKey updated
            {
                sql += "DECLARE @constraint varchar(128);\n" +
                      $"SELECT @constraint = CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = '{mssql.SchemaName}' AND TABLE_NAME = '{tableName}';\n" +
                      $"if (@constraint) IS NOT NULL EXEC('ALTER TABLE [{mssql.SchemaName}].[{tableName}] DROP CONSTRAINT ' + @constraint);\n" +
                      $"ALTER TABLE [{mssql.SchemaName}].[{tableName}] ALTER COLUMN [{colType.Name}] {colType.TypeExpression} NOT NULL;\n" +
                      $"ALTER TABLE [{mssql.SchemaName}].[{tableName}] ADD CONSTRAINT [{CreateConstraintName(colType.Name)}] PRIMARY KEY ([{colType.Name}]);\n";
            }
            else  //Normal colum updated
                sql += $"ALTER TABLE [{mssql.SchemaName}].[{tableName}] ALTER COLUMN [{colType.Name}] {colType.TypeExpression};\n";
        }

        private static void AddSql(List<string> sqls, ref string sql)
        {
            if (!string.IsNullOrEmpty(sql))
            {
                sqls.Add(sql);
                sql = string.Empty;
            }
        }

        private static string CreateConstraintName(string columnName)
        {
            return string.Concat("PK__", columnName, Guid.NewGuid().ToString("N").ToUpper().AsSpan(0, 16));
        }

        private void CreateSchemaIfNotExists()
        {
            mssql.Connection.ExecuteNonQuery($"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{mssql.SchemaName}') BEGIN EXEC('CREATE SCHEMA {mssql.SchemaName}') END");
        }

        private void ExecuteSqls(string sql)
        {
            if (!string.IsNullOrEmpty(sql))
                ExecuteSqls(new string[] { sql });
        }

        private void ExecuteSqls(string[] sqls)
        {
            if (sqls.Any())
            {
                try
                {
                    mssql.ExecuteNonQuery(sqls);
                }
                catch (Exception e)
                {
                    mssql.Log.Add(LogType.Critical, e, "Error in db load: {Message}. Commands: {Commands}", e.Message, sqls);
                    throw new Exception($"Error in db load: {e.Message}. Commands: {sqls}", e);
                }
                mssql.FlushRepoDb();
            }
        }
    }
}