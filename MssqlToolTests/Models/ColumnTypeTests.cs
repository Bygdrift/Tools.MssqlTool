using Bygdrift.Tools.MssqlTool.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MssqlToolTests.Models
{
    [TestClass]
    public class ColumnTypeTests
    {
        [TestMethod]
        public void NotAddedToDbYet()
        {
            var c = new ColumnType("name").AddCsv(typeof(string), 1);
            Assert.AreEqual(c.Change, Change.Add);
            Assert.AreEqual(c.TypeExpression, "varchar(1)");
        }

        [TestMethod]
        public void RemoveFromDb()
        {
            var c = new ColumnType("name").AddSql(SqlType.varchar, 1, null, true);
            Assert.AreEqual(c.Change, Change.None);
            Assert.AreEqual(c.TypeExpression, null);
        }

        [TestMethod]
        public void ChangeTypes()
        {
            //Bit
            var c = new ColumnType("name").AddSql(SqlType.bit, 1, null, true);
            
            c.AddCsv(typeof(bool), 1);
            Assert.AreEqual(c.Change, Change.None);
            Assert.AreEqual(c.TypeExpression, "bit");

            c.AddCsv(typeof(bool), 2);
            Assert.AreEqual(c.Change, Change.None);
            Assert.AreEqual(c.TypeExpression, "bit");

            c.AddCsv(typeof(int), 1);
            Assert.AreEqual(c.Change, Change.Upgrade);
            Assert.AreEqual(c.TypeExpression, "int");

            c.AddCsv(typeof(string), 1);
            Assert.AreEqual(c.Change, Change.Upgrade);
            Assert.AreEqual(c.TypeExpression, "varchar(1)");

            //Int
            c.AddSql(SqlType.@int, 1, null, true);
            
            c.AddCsv(typeof(bool), 1);
            Assert.AreEqual(c.Change, Change.Downgrade);
            Assert.AreEqual(c.TypeExpression, "bit");

            c.AddCsv(typeof(int), 1);
            Assert.AreEqual(c.Change, Change.None);
            Assert.AreEqual(c.TypeExpression, "int");

            c.AddCsv(typeof(int), 2);
            Assert.AreEqual(c.Change, Change.None);
            Assert.AreEqual(c.TypeExpression, "int");

            c.AddCsv(typeof(float), 1);
            Assert.AreEqual(c.Change, Change.Upgrade);
            Assert.AreEqual(c.TypeExpression, "real");

            c.AddCsv(typeof(decimal), 1);
            Assert.AreEqual(c.Change, Change.Upgrade);
            Assert.AreEqual(c.TypeExpression, "decimal(18,12)");

            c.AddCsv(typeof(string), 1);
            Assert.AreEqual(c.Change, Change.Upgrade);
            Assert.AreEqual(c.TypeExpression, "varchar(1)");

            //Varchar
            c.AddSql(SqlType.varchar, 1, null, true);
            
            c.AddCsv(typeof(bool), 1);
            Assert.AreEqual(c.Change, Change.Downgrade);
            Assert.AreEqual(c.TypeExpression, "bit");

            c.AddCsv(typeof(int), 1);
            Assert.AreEqual(c.Change, Change.Downgrade);
            Assert.AreEqual(c.TypeExpression, "int");

            c.AddCsv(typeof(float), 1);
            Assert.AreEqual(c.Change, Change.Downgrade);
            Assert.AreEqual(c.TypeExpression, "real");

            c.AddCsv(typeof(decimal), 1);
            Assert.AreEqual(c.Change, Change.Downgrade);
            Assert.AreEqual(c.TypeExpression, "decimal(18,12)");

            c.AddCsv(typeof(string), 1);
            Assert.AreEqual(c.Change, Change.None);
            Assert.AreEqual(c.TypeExpression, "varchar(1)");

            c.AddCsv(typeof(string), 2);
            Assert.AreEqual(c.Change, Change.Equal);
            Assert.AreEqual(c.TypeExpression, "varchar(2)");
        }
    }
}
