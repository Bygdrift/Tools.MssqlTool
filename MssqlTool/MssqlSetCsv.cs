using Bygdrift.Tools.CsvTool;
using Bygdrift.Tools.MssqlTool.Helpers;
using Bygdrift.Tools.MssqlTool.Models;
using RepoDb;
using System;
using System.Collections.Generic;

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
            var errors = new Errors(Log);
            if (!PrepareData(csv, removeEmptyColumns))
                return null;

            if (truncateTable)
                DeleteTable(tableName);

            new PrepareTableForCsv(this, csv, tableName, null);
            var data = csv.ToExpandoList();

            try
            {
                if (csv.ColCount * csv.RowCount < 2000)
                    Connection.InsertAll($"[{SchemaName}].[{tableName}]", data, csv.RowLimit.Max, commandTimeout: 3600);
                else
                    Connection.BulkInsert($"[{SchemaName}].[{tableName}]", data, bulkCopyTimeout: 3600);
            }
            catch (Exception e)
            {
               errors.AddErrors(e.Message);
            }
            return errors.GetErrors;
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

            var errors = new Errors(Log);
            if (!PrepareData(csv, removeEmptyColumns))
                return null;

            if (primaryKey == null)
                return errors.AddErrors("PrimaryKey cannot be set to null. Use the InsertCsv() method instead.");

            var validation = ValidatePrimaryKey(csv, tableName, primaryKey);
            if (validation != null)
                return errors.AddErrors(validation);

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
                   errors.AddErrors(e.Message);
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
                   errors.AddErrors(e.Message);
                }
            }

            return errors.GetErrors;
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
