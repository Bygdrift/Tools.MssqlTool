using Bygdrift.Tools.CsvTool;
using Bygdrift.Tools.LogTool;
using Bygdrift.Tools.LogTool.Models;
using Bygdrift.Tools.MssqlTool.Models;
using RepoDb;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Bygdrift.Tools.MssqlTool
{
    /// <summary>
    /// Access to edit Microsoft SQL database data
    /// </summary>
    public partial class Mssql
    {
        /// <summary>
        /// Excecutes a SQL
        /// </summary>
        public string ExecuteNonQuery(IEnumerable<string> sqls)
        {
            var res = string.Empty;
            foreach (var item in sqls)
            {
                var output = ExecuteNonQuery(item);
                if (output != null)
                    res += output + "\n";
            }
            return res;
        }

        /// <summary>
        /// Excecutes a SQL
        /// </summary>
        public string ExecuteNonQuery(string sql)
        {
            try
            {
                Connection.ExecuteNonQuery(sql);
            }
            catch (Exception e)
            {
                Log.LogError(e, "Mssql error in ExecuteNonQuery, running {Sql}. Error: {E}", sql, e.Message);
                return e.Message;
            }
            return null;
        }

        /// <summary>
        /// Excecutes a SQL
        /// </summary>
        public string ExecuteNonQuery(string sql, dynamic param)
        {
            try
            {
                Connection.ExecuteNonQuery(sql, (object)param);
            }
            catch (Exception e)
            {
                Log.LogError(e, "Mssql error in ExecuteNonQuery, running {Sql}. Error: {E}", sql, e.Message);
                return e.Message;
            }
            return null;
        }

        /// <summary>
        /// If a column in db is varchar but only continas int data and this csv says that it should be an int, then this method will try to update columntype to an int
        /// </summary>
        public bool TryChangeDbColumnTypes(string tableName, Csv csv)
        {
            var columns = GetColumnTypes(tableName).ToList();
            if (columns.Count == 0)
                return true;

            ColumnType.AddCsv(csv, null, columns);
            var res = true;
            foreach (var item in columns)
                if (!TryChangeDbColumnType(tableName, item))
                    res = false;

            return res;
        }

        /// <summary>
        /// If a column in db is varchar but only continas int data and this csv says that it should be an int, then this method will try to update columntype to an int
        /// </summary>
        public bool TryChangeDbColumnTypes(string tableName, List<ColumnType> columns)
        {
            var res = true;
            foreach (var item in columns.Where(o=> o.Change == Change.Downgrade))
                if (!TryChangeDbColumnType(tableName, item))
                    res = false;

            return res;
        }

        private bool TryChangeDbColumnType(string tableName, ColumnType column)
        {
            if (column.Change != Change.Downgrade)
                return true;

            var sql = $"ALTER TABLE [{SchemaName}].[{tableName}] ALTER COLUMN [{column.Name}] {column.TypeExpression};";
            try
            {
                Connection.ExecuteNonQuery(sql);
            }
            catch (Exception e)
            {
                return false;
            }
            return true;
        }

        public string RemoveEmptyColumns(string tableName)
        {
            var sql = "DECLARE @sql NVARCHAR(MAX)\n" +
                      "SELECT @sql = ISNULL(@sql + 'UNION ALL', '') + '\n" +
                      "SELECT ''' + COLUMN_NAME + ''' AS col FROM ' + TABLE_SCHEMA + '.' + TABLE_NAME + ' HAVING COUNT(' + COLUMN_NAME + ') = 0 '\n" +
                      "FROM INFORMATION_SCHEMA.COLUMNS\n" +
                      $"WHERE TABLE_SCHEMA = '{SchemaName}' AND TABLE_NAME = '{tableName}'\n" +
                      "EXEC(@SQL)\n";

            var columns = Connection.ExecuteQuery(sql).Select(o => o.col as string);
            return RemoveColumns(tableName, columns?.ToArray());
        }

        /// <summary>
        /// Remove column. If it's a primarey key, then the constraint are also removed
        /// </summary>
        public string RemoveColumn(string tableName, string column)
        {
            var columns = new List<string> { column };
            return RemoveColumns(tableName, columns.ToArray());
        }

        /// <summary>
        /// Removes columns. If they are primarey keys, then the constraint are also removed
        /// </summary>
        public string RemoveColumns(string tableName, string[] columns)
        {
            if (columns.Any())
            {
                var sql = "DECLARE @PrimaryKey nvarchar(128), @Constraint varchar(128);\n" +
                          $"SELECT @PrimaryKey = COLUMN_NAME, @Constraint = CONSTRAINT_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = '{SchemaName}' AND TABLE_NAME = '{tableName}';\n";

                foreach (var col in columns)
                {
                    sql += $"IF('{col}' = @PrimaryKey) EXEC('ALTER TABLE [{SchemaName}].[{tableName}] DROP CONSTRAINT ' + @Constraint);\n";
                    sql += $"ALTER TABLE [{SchemaName}].[{tableName}] DROP COLUMN [{col}];\n";
                }
                return ExecuteNonQuery(sql);
            }
            return null;
        }


        /// <summary>
        /// Validates if there are any duplicates in the primaryKey or if there are any nulls.
        /// </summary>
        /// <returns>Null if no errors or else an array of errors. Errors are also send to AppBase</returns>
        public Log ValidatePrimaryKey(Csv csv, string tableName, string primaryKey)
        {
            var subLog = new Log(Log.Logger);
            if (!csv.Headers.Any())
                return subLog.Add(LogType.Error, "The csv is empty.");

            var colRecords = csv.GetColRecords(primaryKey, true);
            if (colRecords == null)
                subLog.Add(LogType.Error, $"The primaryKey '{primaryKey}' in the table '{tableName}' does not exist.");
            else if (csv.RowCount > 0)
            {
                var duplicates = colRecords.GroupBy(o => o.Value).Where(g => g.Count() > 1).Select(y => y.Key).ToArray();
                if (duplicates.Any())
                {
                    var duplicatesString = string.Join(',', duplicates.ToArray());
                    subLog.Add(LogType.Error, $"There must not be any duplicates in the column with the header '{primaryKey}' because it is set as a primary key in the database. There are {duplicates.Length} duplicates. They are: '{duplicatesString}'.");
                }

                var nulls = colRecords.Where(o => o.Value == null).ToList();
                if (nulls.Count > 0)
                    subLog.Add(LogType.Error, $"There must not be any null values in the column with the header '{primaryKey}' because it is set as a primary key in the database. There are {nulls} nulls.");
            }

            return subLog;
        }
    }
}
