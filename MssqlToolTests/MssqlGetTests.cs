using Bygdrift.Tools.CsvTool;
using Bygdrift.Tools.MssqlTool;
using Bygdrift.Tools.MssqlTool.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;

namespace MssqlToolTests
{
    [TestClass]
    public class MssqlGetTests : BaseTests
    {
        [TestMethod]
        public void ValidatePrimaryKey()
        {
            var csv = new Csv("Id, Name").AddRow(1).AddRow(2).AddRow(2).AddRecord(4, 1, null);

            var log1 = Mssql.ValidatePrimaryKey(csv, MethodName, "Id");
            Assert.IsTrue(log1.GetLogs().Count() == 2);

            var log2 = Mssql.ValidatePrimaryKey(csv, MethodName, "NotExisting");
            Assert.IsTrue(log2.GetLogs().Count() == 1);

            var csv2 = new Csv("Id, Name");
            var log3 = Mssql.ValidatePrimaryKey(csv2, MethodName, "Id");
            Assert.IsFalse(log3.HasErrorsOrCriticals());
        }

        [TestMethod]
        public void GetAsCsv()
        {
            var csv = new Csv("Id, Data, Date, age")
                .AddRow(new Random().Next(1, 5000), "Some text", DateTime.Now, 22)
                .AddRow(new Random().Next(1, 5000), "Some new text", DateTime.Now, 23);
            
            Assert.IsNull(Mssql.MergeCsv(csv, MethodName, "Id"));
            var csvFromReader = Mssql.GetAsCsv(MethodName);
            Assert.IsTrue(csvFromReader.Headers.Count == 4);
            var csvFromReader2 = Mssql.GetAsCsv(MethodName, "Id", "Data");
            Assert.IsTrue(csvFromReader2.Headers.Count == 2);
        }

        [TestMethod]
        public void GetColumnTypes()
        {
            var csv = new Csv("Id, Data, Date, age, decimal")
                .AddRow(new Random().Next(1, 5000), "Some text", DateTime.Now, 22, 36.8);

            Assert.IsNull(Mssql.MergeCsv(csv, MethodName, "Id"));
            var res = Mssql.GetColumnTypes(MethodName).ToList();
            Assert.AreEqual(res[0].TypeNameSql, SqlType.@int);
            Assert.AreEqual(res[1].TypeNameSql, SqlType.varchar);
            Assert.AreEqual(res[2].TypeNameSql, SqlType.datetime);
            Assert.AreEqual(res[3].TypeNameSql, SqlType.@int);
            Assert.AreEqual(res[4].TypeNameSql, SqlType.@float);
        }

        [TestMethod]
        public void SetReadDeletes()
        {
            var csv = new Csv("Id, Data, Date, age")
                .AddRow(1, "Some text", DateTime.Now, 22)
                .AddRow(2, "Some new text", DateTime.Now, 23);

            Assert.IsNull(Mssql.MergeCsv(csv, MethodName, "Id"));
            var resCsv = Mssql.GetAsCsv(MethodName);
            Assert.AreEqual(resCsv.ColLimit, (1, 4));
            Assert.AreEqual(resCsv.RowLimit, (1, 2));
            Assert.AreEqual(resCsv.GetRecord(1, 2), "Some text");
        }
    }
}