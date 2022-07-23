using Bygdrift.Tools.LogTool;
using Bygdrift.Tools.MssqlTool;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Diagnostics;
using System.IO;

namespace MssqlToolTests
{
    public class BaseTests
    {

        public string MethodName
        {
            get
            {
                var methodInfo = new StackTrace().GetFrame(1).GetMethod();
                return methodInfo.Name;
            }
        }


        /// <summary>Path to project base</summary>
        public static readonly string BasePath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..\\..\\..\\"));
        public readonly Mssql Mssql;

        public BaseTests()
        {
            var dbPath = Path.Combine(BasePath, "Files\\DB\\MssqlTools.mdf");
            var conn = $"Data Source=(LocalDB)\\MSSQLLocalDB;AttachDbFilename={dbPath};Integrated Security=True";
            Mssql = new Mssql(conn, "Test", new Log());
        }

        public string Cleanup()
        {
            var methodInfo = new StackTrace().GetFrame(1).GetMethod();
            return Cleanup(methodInfo.Name);
        }

        public string Cleanup(string tableName)
        {
            var res = Mssql.DeleteTable(tableName);
            Assert.IsNull(res);
            Mssql.Dispose();
            return res;
        }
    }
}
