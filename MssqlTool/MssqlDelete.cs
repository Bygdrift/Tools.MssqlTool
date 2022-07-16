using RepoDb;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bygdrift.Tools.MssqlTool
{
    public partial class Mssql
    {
        /// <summary>
        /// Removes rows that are older than a given expiration data
        /// </summary>
        /// <param name="tableName">The table name</param>
        /// <param name="columnName">The name on the column that contains dates that should be evaluated, wether they are expired</param>
        /// <param name="expirationTime">The expiration time - all older than this date, is removed.</param>
        /// <returns>Null if no errors or else an array of errors. Errors are also send to AppBase</returns>
        public string[] DeleteOldRows(string tableName, string columnName, DateTime expirationTime)
        {
            try
            {
                var date = expirationTime.ToString("s");
                var sql = $"IF OBJECT_ID('{SchemaName}.{tableName}') IS NULL BEGIN SELECT 0 END; ELSE BEGIN SELECT 1 DELETE FROM[{SchemaName}].[{tableName}] WHERE[{columnName}] < '{date}'; END;";
                Connection.ExecuteQuery(sql);
            }
            catch (Exception e)
            {
                Log.LogError(e, "Error while removing rows: {E}", e.Message);
                return new string[] { e.Message };
            }
            return default;
        }

        /// <summary>
        /// Delets the table if exists
        /// </summary>
        public string DeleteTable(string tableName)
        {
            try
            {
                Connection.ExecuteNonQuery($"DROP TABLE IF EXISTS [{SchemaName}].[{tableName}]");
            }
            catch (Exception e)
            {
                Log.LogError(e, "Mssql error in DeleteTable: {E}", e.Message);
                //if(e is SqlException)
                return e.Message;
            }
            return null;
        }

        /// <summary>
        /// Empties the table for data if exists
        /// </summary>
        /// <returns>Null if no errors or else a string explaining the error</returns>
        public string TruncateTable(string tableName)
        {
            try
            {
                Connection.DeleteAll($"[{SchemaName}].[{tableName}]", commandTimeout: 3600);
            }
            catch (Exception e)
            {
                Log.LogError(e, "Mssql error in TruncateTable: {E}", e.Message);
                return e.Message;
            }
            return null;
        }

    }
}
