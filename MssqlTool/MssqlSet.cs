using Bygdrift.Tools.CsvTool;
using Bygdrift.Tools.LogTool;
using Bygdrift.Tools.LogTool.Models;
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
