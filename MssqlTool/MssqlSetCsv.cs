using Bygdrift.Tools.CsvTool;
using Bygdrift.Tools.LogTool;
using Bygdrift.Tools.LogTool.Models;
using Bygdrift.Tools.MssqlTool.Helpers;
using RepoDb;
using System;
using System.Linq;

namespace Bygdrift.Tools.MssqlTool
{
    /// <summary>
    /// Access to edit Microsoft SQL database data
    /// </summary>
    public partial class Mssql
    {

        /// <summary>
        /// Data will be inserted in the table and if there are no table, it will be created.
        /// If a column types or names has been changed, it will be managed.
        /// </summary>
        /// <param name="csv"></param>
        /// <param name="tableName"></param>
        /// <param name="truncateTable">If true, the table gets truncated and filed with new data</param>
        /// <param name="removeEmptyColumns">If true, all columns that only contains null data, will be removed</param>
        /// <returns>Null if no errors or else an array of errors. Errors are also send to AppBase</returns>
        public string[] InsertCsv(Csv csv, string tableName, bool truncateTable = false, bool removeEmptyColumns = false)
        {
            if (!PrepareData(csv, removeEmptyColumns))
                return null;

            var subLog = new Log(Log.Logger);  //Generated as a sub log so result from current method can be returned

            if (truncateTable)
                DeleteTable(tableName);

            new PrepareTableForCsv(this, csv, tableName, null);
            var data = csv.ToExpandoList();

            try
            {
                Connection.BulkInsert($"[{SchemaName}].[{tableName}]", data, bulkCopyTimeout: 3600);
            }
            catch (Exception e)
            {
                subLog.Add(LogType.Error, e.Message);
            }

            return subLog.Any() ? subLog.GetLogs().ToArray() : null;
        }

        /// <summary>
        /// Data will be merged into the table and if there are no table, it will be created.
        /// If column types or names has been changed, it will be managed.
        /// Right now, this method can only alter columns one time and then merge data. So it cannot: Merge, Alter, Merge. But it can: Alter, Merge, Merge... Merge
        /// </summary>
        /// <param name="csv"></param>
        /// <param name="tableName"></param>
        /// <param name="primaryKey">Cannot be null - use the InsertCsv() method instead. If set, this column can't be null and must be unique values. If set and you try to insert a row that has an id that are already present, then the row will be updated</param>
        /// <param name="truncateTable">If true, the table gets truncated and filed with new data</param>
        /// <param name="removeEmptyColumns">If true, all columns that only contains null data, will be removed</param>
        /// <returns>Null if no errors or else an array of errors. Errors are also send to AppBase.Log</returns>
        public string[] MergeCsv(Csv csv, string tableName, string primaryKey, bool truncateTable, bool removeEmptyColumns = false)
        {
            if (csv == null | csv.RowCount == 0)
                return null;

            var subLog = new Log(Log.Logger);  //Generated as a sub log so result from current method can be returned
            if (!PrepareData(csv, removeEmptyColumns))
                return null;

            if (primaryKey == null)
            {
                subLog.LogError("PrimaryKey cannot be set to null. Use the InsertCsv() method instead.");
                return subLog.GetLogs().ToArray();
            }

            var validation = ValidatePrimaryKey(csv, tableName, primaryKey);
            if (validation.Logs.Any())
                return subLog.Add(validation).GetLogs().ToArray();

            if (truncateTable)
                DeleteTable(tableName);

            new PrepareTableForCsv(this, csv, tableName, primaryKey);
            var data = csv.ToExpandoList();

            if (truncateTable)
            {
                try
                {
                    Connection.BulkInsert($"[{SchemaName}].[{tableName}]", data, bulkCopyTimeout: 3600);
                }
                catch (Exception e)
                {
                    subLog.Add(LogType.Error, e.Message);
                }
            }
            else
            {
                try
                {
                    Connection.BulkMerge($"[{SchemaName}].[{tableName}]", data, bulkCopyTimeout: 3600);
                }
                catch (Exception e)
                {
                    subLog.Add(LogType.Error, e.Message);
                }
            }

            return subLog.Any() ? subLog.GetLogs().ToArray() : null;
        }

        /// <returns>False if there is no content</returns>
        private static bool PrepareData(Csv csv, bool removeEmptyColumns)
        {
            //csv.UniqueHeadersIgnoreCase(true);
            if (removeEmptyColumns)
                csv.RemoveEmptyColumns();

            if (csv.Headers.Count == 0)
                return false;

            for (int c = csv.ColLimit.Min; c < csv.ColLimit.Max; c++)  //Brackets cannot be in the column name
                csv.Headers[c] = csv.Headers[c].Replace("[", "(").Replace("]", ")");

            return true;
        }


    }
}
