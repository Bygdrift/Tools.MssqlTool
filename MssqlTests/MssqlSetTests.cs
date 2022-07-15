using Bygdrift.CsvTools;
using Bygdrift.MssqlTools;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;

namespace MssqlTests
{
    [TestClass]
    public class MssqlSetCsvTests : BaseTests
    {
        [TestMethod]
        public void RemoveOldRows()
        {
            var table = "RemoveOldRows";
            var csv = new Csv("Id, Date").AddRow(1, DateTime.Now).AddRow(2, DateTime.Now.AddMonths(-5)).AddRow(3, DateTime.Now.AddMonths(-10));

            Assert.IsNull(Mssql.DeleteTable(table));
            Assert.IsNull(Mssql.MergeCsv(csv, table, "Id", false, false));
            Assert.IsNull(Mssql.RemoveOldRows(table, "Date", DateTime.Now.AddMonths(-6)));
            var csvFromReader = Mssql.GetAsCsv(table);
            Assert.IsTrue(csvFromReader.Records.Count == 4);

            Assert.IsFalse(Mssql.Log.GetErrorsAndCriticals().Any());
            Mssql.Dispose();
        }


        [TestMethod]
        public void TruncateTable()
        {
            var table = "TruncateTable";
            var csv = new Csv("Id, Data, Date, Age").AddRow(1, "Some text", DateTime.Now, 22);

            Assert.IsNull(Mssql.DeleteTable(table));
            Assert.IsNull(Mssql.MergeCsv(csv, table, "Id", false, false));
            Assert.IsNull(Mssql.TruncateTable(table));
            var csvFromReader = Mssql.GetAsCsv(table);
            Assert.IsTrue(csvFromReader.Records.Count == 0);
            Assert.IsFalse(Mssql.Log.GetErrorsAndCriticals().Any());
            Mssql.Dispose();
        }
    }
}