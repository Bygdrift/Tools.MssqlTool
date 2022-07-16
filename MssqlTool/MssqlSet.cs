﻿using Bygdrift.Tools.CsvTool;
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
        /// Validates if there are any duplicates in the primaryKey or if there are any nulls.
        /// </summary>
        /// <returns>Null if no errors or else an array of errors. Errors are also send to AppBase</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "<Pending>")]
        public string[] ValidatePrimaryKey(Csv csv, string tableName, string primaryKey)
        {
            if (!csv.Headers.Any())
                return new string[] { "The csv is empty." };

            var res = new List<string>();

            var colRecords = csv.GetColRecords(primaryKey, true);
            if (colRecords == null)
                res.Add($"The primaryKey '{primaryKey}' in the table '{tableName}' does not exist.");
            else if (csv.RowCount > 0)
            {
                var duplicates = colRecords.GroupBy(o => o.Value).Where(g => g.Count() > 1).Select(y => y.Key).ToArray();
                if (duplicates.Any())
                {
                    var duplicatesString = string.Join(',', duplicates.ToArray());
                    res.Add($"There must not be any duplicates in the column with the header '{primaryKey}' because it is set as a primary key in the database. There are {duplicates.Length} duplicates. They are: '{duplicatesString}'.");
                }

                var nulls = colRecords.Where(o => o.Value == null).ToList();
                if (nulls.Count > 0)
                    res.Add($"There must not be any null values in the column with the header '{primaryKey}' because it is set as a primary key in the database. There are {nulls} nulls.");
            }

            return res.Any() ? res.ToArray() : null;
        }
    }
}