using Bygdrift.Tools.CsvTool;
using Bygdrift.Tools.MssqlTool;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;

namespace MssqlToolTests
{
    [TestClass]
    public class MssqlSetCsvTests : BaseTests
    {
        [TestMethod]
        public void RemoveOldRows()
        {
            var csv = new Csv("Id, Date")
                .AddRow(1, DateTime.Now)
                .AddRow(2, DateTime.Now.AddMonths(-5))
                .AddRow(3, DateTime.Now.AddMonths(-10));
            
            Assert.IsNull(Mssql.DeleteTable(MethodName));
            Assert.IsNull(Mssql.MergeCsv(csv, MethodName, "Id", false, false));
            Assert.IsNull(Mssql.DeleteOldRows(MethodName, "Date", DateTime.Now.AddMonths(-6)));
            var csvFromReader = Mssql.GetAsCsv(MethodName);
            Assert.IsTrue(csvFromReader.Records.Count == 4);

            Assert.IsFalse(Mssql.Log.GetErrorsAndCriticals().Any());
            Mssql.Dispose();
        }


        [TestMethod]
        public void TruncateTable()
        {
            var csv = new Csv("Id, Data, Date, Age").AddRow(1, "Some text", DateTime.Now, 22);

            Assert.IsNull(Mssql.DeleteTable(MethodName));
            Assert.IsNull(Mssql.MergeCsv(csv, MethodName, "Id", false, false));
            Assert.IsNull(Mssql.TruncateTable(MethodName));
            var csvFromReader = Mssql.GetAsCsv(MethodName);
            Assert.IsTrue(csvFromReader.Records.Count == 0);
            Assert.IsFalse(Mssql.Log.GetErrorsAndCriticals().Any());
            Mssql.Dispose();
        }
    }
}