using Bygdrift.Tools.LogTool;
using Bygdrift.Tools.MssqlTool;
using System;
using System.IO;

namespace MssqlToolTests
{
    public class BaseTests
    {
        public readonly Mssql Mssql;

        public BaseTests()
        {
            var dbPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..\\..\\..\\Files\\DB\\MssqlTools.mdf"));
            var conn = $"Data Source=(LocalDB)\\MSSQLLocalDB;AttachDbFilename={dbPath};Integrated Security=True";
            Mssql = new Mssql(conn, "Test", new Log());
        }

    }
}
