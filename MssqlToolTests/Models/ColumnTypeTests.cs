﻿using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MssqlToolTests.Models
{
    [TestClass]
    public class ColumnTypeTests
    {
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