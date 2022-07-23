using Bygdrift.Tools.CsvTool;
using Bygdrift.Tools.MssqlTool.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MssqlToolTests.Models
{
    [TestClass]
    public class ColumnTypeTests:BaseTests
    {
        ///// <summary>
        ///// Check if GetConstraint is working properly
        ///// </summary>
        //[TestMethod]
        //public void GetConstraint()
        //{
        //    var csv = new Csv("Id, Data").AddRow(1, 1);
        //    Assert.IsNull(Mssql.DeleteTable(MethodName));
        //    Assert.IsNull(Mssql.MergeCsv(csv, MethodName, "Id", false, false));
        //    var prep = new PrepareTableForCsv(Mssql, csv, MethodName, "Id");
        //    var constraint = prep.GetConstraint();
        //    Assert.IsTrue(constraint.StartsWith("PK__GetConst"));
        //    Cleanup();
        //}

        [TestMethod]
        public void ChangeTypes()
        {
            //var colType = new ColumnType("name", "int", 1, false), typeof(int), 1, false);
            //Assert.IsFalse(colType.TryGetUpdatedChangedType(out _));

            //colType = new ColumnType"name", "int", 1, false), typeof(long), 1, false);
            //Assert.IsTrue(colType.TryGetUpdatedChangedType(out string res) && res == "bigint");

            //colType = new ColumnType("name", "bigInt", 1, false), typeof(int), 1, false);
            //Assert.IsFalse(colType.TryGetUpdatedChangedType(out _));

            //colType = new ColumnType("name", "int", 1, false, typeof(decimal), 1, false);
            //Assert.IsTrue(colType.TryGetUpdatedChangedType(out res) && res == "decimal(18,12)");
        }
    }
}
