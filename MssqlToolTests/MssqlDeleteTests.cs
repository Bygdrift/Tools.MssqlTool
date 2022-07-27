using Bygdrift.Tools.CsvTool;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq;

namespace MssqlToolTests
{
    [TestClass]
    public class MssqlDeleteTests : BaseTests
    {
        [TestMethod]
        public void RemoveEmptyColumns()
        {
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id, Data, B, C").AddRow(1, 3, null, null), MethodName, "Id"));
            Assert.IsNull(Mssql.RemoveEmptyColumns(MethodName));
            Assert.AreEqual(Mssql.GetColumnTypes(MethodName).Count(), 2);
        }

        [TestMethod]
        public void RemoveColumn()
        {
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id, A, B, C").AddRow(1, 3, null, null), MethodName, "Id"));
            Assert.IsNull(Mssql.RemoveColumn(MethodName, "B"));  //Removes a normal column
            Assert.AreEqual(Mssql.GetColumnTypes(MethodName).Count(), 3);

            Assert.IsNull(Mssql.RemoveColumn(MethodName, "Id"));  //Removes primary key
            Assert.AreEqual(Mssql.GetColumnTypes(MethodName).Count(), 2);
        }

        [TestMethod]
        public void RemoveColumns()
        {
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id, A, B, C").AddRow(1, 3, null, null), MethodName, "Id"));
            var errors = Mssql.RemoveColumns(MethodName, new string[] { "B", "C" });  //Removes two normal columns
            var cols = Mssql.GetColumnTypes(MethodName);
            Assert.AreEqual(cols.Count(), 2);
        }
    }
}
