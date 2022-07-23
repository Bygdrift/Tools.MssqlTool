using Bygdrift.Tools.CsvTool;
using Bygdrift.Tools.MssqlTool;
using Bygdrift.Tools.MssqlTool.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace MssqlToolTests
{
    [TestClass]
    public class MssqlSetTests :BaseTests
    {

        [TestInitialize]
        public void TestInit()
        {
        }

        [TestCleanup]
        public void TestCleanup()
        {
        }

        [TestMethod]
        public void TruncateTable()
        {
            var csvOne = new Csv("Id").AddRow(1);
            var csvTwo = new Csv("Id").AddRow(2);

            Assert.IsNull(Mssql.DeleteTable(MethodName));
            Assert.IsNull(Mssql.MergeCsv(csvOne, MethodName, "Id", false, false));
            Assert.IsNull(Mssql.MergeCsv(csvTwo, MethodName, "Id", true, false));

            var csvFromReader = Mssql.GetAsCsv(MethodName);
            Assert.AreEqual(csvFromReader.RowCount, 1);
            Assert.IsFalse(Mssql.Log.GetErrorsAndCriticals().Any());
            Cleanup();
        }

        //Denne virker ikke og skal komme til at virke - 1. prio
        [TestMethod]
        public void ChangeColumnType()
        {
            Assert.IsNull(Mssql.DeleteTable(MethodName));
            Assert.IsNull(Mssql.InsertCsv(new Csv("Id, Data").AddRow(1, "T"), MethodName, false, false));
            Assert.IsNull(Mssql.InsertCsv(new Csv("Id, Data").AddRow(1, 1), MethodName, false, false));
            Assert.AreEqual(Mssql.GetColumnTypes(MethodName).Last().TypeNameSql, SqlType.@int);

            Assert.IsNull(Mssql.InsertCsv(new Csv("Id, Data").AddRow(1, "T"), MethodName, false, false));
            Assert.IsNull(Mssql.InsertCsv(new Csv("Id, Data").AddRow(1, 1), MethodName, true, false));  //When truncating, the whole table is deleted, so the column Data is change
            Assert.AreEqual(Mssql.GetColumnTypes(MethodName).Last().TypeNameSql, SqlType.@int);
            Cleanup();
        }


        //Denne virker ikke og skal komme til at virke - 1. prio
        [TestMethod]
        public void DeleteEmptyColumn()
        {
            //This makes only one column:
            Assert.IsNull(Mssql.DeleteTable(MethodName));
            Assert.IsNull(Mssql.InsertCsv(new Csv("Id, Data").AddRow(1, null), MethodName, false, true));
            Assert.AreEqual(Mssql.GetColumnTypes(MethodName).Count(), 1);

            //This makes two columns:
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id, Data").AddRow(1, 1), MethodName, "Id", false, true));
            Assert.AreEqual(Mssql.GetColumnTypes(MethodName).Count(), 2);
                        
            //This will remove the column that only has null now:
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id, Data").AddRow(1, null), MethodName, "Id", false, true));
            var csvFromReader = Mssql.GetAsCsv(MethodName);

            Assert.AreEqual(Mssql.GetColumnTypes(MethodName).Count(), 1);
            Cleanup();
        }

        //Denne virker ikke og skal komme til at virke - 1. prio
        [TestMethod]
        public void PrimaryKey_Added()
        {
            Assert.IsNull(Mssql.DeleteTable(MethodName));
            Assert.IsNull(Mssql.InsertCsv(new Csv("Id").AddRow(1), MethodName, false));
            Assert.IsNull(Mssql.InsertCsv(new Csv("Id, Data").AddRow(null, 1), MethodName, false));  //This gives a null value to a column that sholud be tried to convert into a primary key!
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id").AddRow(1), MethodName, "Id", false));
            Cleanup();
        }

        //Denne virker ikke og skal komme til at virke - 1. prio
        [TestMethod]
        public void ChangePrimaryKey_Changed()
        {
            Assert.IsNull(Mssql.DeleteTable(MethodName));
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id1, Id2").AddRow(1, 1), MethodName, "Id1", false));
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id1, Id2").AddRow(1, 1), MethodName, "Id2", false));
            Cleanup();
        }

        //Denne virker ikke og skal komme til at virke - 1. prio
        [TestMethod]
        public void PrimaryKey_Removed()
        {
            Assert.IsNull(Mssql.DeleteTable(MethodName));
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id").AddRow(1), MethodName, "Id", false));
            Assert.IsNull(Mssql.InsertCsv(new Csv("Id").AddRow(1), MethodName, false));
            Cleanup();
        }



        /// <summary>
        /// this would fail without flush fail and I asked for a solution here: https://stackoverflow.com/questions/69635893/repodb-doesnt-merge-correct-after-altering-a-column
        /// If RepoDB didn't get flushed, the reulst would be: Id = 1 Data = "Lo", Name = NULL
        /// </summary>
        [TestMethod]
        public void CreateAndTruncateTable_Fails()
        {
            var csvOne = new Csv("Id, Data, Date, Age").AddRow(1, "Some text", DateTime.Now, 22);
            var csvTwo = new Csv("Id, Data, Date, Age, Name").AddRow(1, "Some more text", DateTime.Now, 22, "Knud");

            Assert.IsNull(Mssql.DeleteTable(MethodName));
            Assert.IsNull(Mssql.MergeCsv(csvOne, MethodName, "Id", false, false));
            Assert.IsNull(Mssql.MergeCsv(csvTwo, MethodName, "Id", false, false));

            var csvFromReader = Mssql.GetAsCsv(MethodName);
            Assert.AreEqual(csvFromReader.GetRecord(1, 2), "Some more text");
            Assert.AreEqual(csvFromReader.GetRecord(1, 5), "Knud");

            Assert.IsFalse(Mssql.Log.GetErrorsAndCriticals().Any());
            Cleanup();
        }

        /// <summary>
        /// If there are a column left that are all empty, it should be removed
        /// </summary>
        [TestMethod]
        public void IfColumnsGetsRemoved()
        {
            var csv = new Csv("Id, Data, Date, Age, EmptyColumn").AddRow(1, "Some text", DateTime.Now, 22);
            
            Assert.IsNull(Mssql.DeleteTable(MethodName));
            Assert.IsNull(Mssql.MergeCsv(csv, MethodName, "Id", false, true));

            var csvFromReader = Mssql.GetAsCsv(MethodName);
            Assert.IsFalse(csvFromReader.TryGetColId("EmptyColumn", out int _));
            Cleanup();
        }

        [TestMethod]
        public void ChangeIntToVarchar()
        {
            Assert.IsNull(Mssql.DeleteTable(MethodName));
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id, Data").AddRow("A", 5), MethodName, "Id", false, false));
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id, Data").AddRow("A", "AB"), MethodName, "Id", false, false));
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id, Data").AddRow("A", 6), MethodName, "Id", false, false));
            var columns = Mssql.GetColumnTypes(MethodName).ToList();
            Assert.AreEqual(columns[1].TypeNameSql, SqlType.varchar);

            Assert.IsNull(Mssql.DeleteTable(MethodName));
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id, Data").AddRow("A", "AB"), MethodName, "Id", false, false));
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id, Data").AddRow("A", 6), MethodName, "Id", false, false));
            columns = Mssql.GetColumnTypes(MethodName).ToList();
            Assert.AreEqual(columns[1].TypeNameSql, SqlType.varchar);

            Cleanup();
        }

        [TestMethod]
        public void InsertCsvThenMergeFailsOnPrimaryKeay()
        {
            Assert.IsNull(Mssql.DeleteTable(MethodName));
            Assert.IsNull(Mssql.InsertCsv(new Csv("Id, Data").AddRow(1, "Some text"), MethodName, false, false));
            try
            {
                Mssql.MergeCsv(new Csv("Id, Data").AddRow(1, "Some more text"), MethodName, "Id", false, false);
            }
            catch (Exception)
            {
            }
            Assert.AreEqual(Mssql.Log.GetErrorsAndCriticals().Count(), 2);
            Cleanup();
        }

        [TestMethod]
        public void LoadMuchData()
        {
            var path = Path.Combine(BasePath, "Files", "Csv", "Height and weight for 25000 persons.csv");
            var csv = new Csv().FromCsvFile(path);

            Assert.IsNull(Mssql.DeleteTable(MethodName));
            Assert.IsNull(Mssql.MergeCsv(csv, MethodName, "Index", false, false));
            Assert.IsFalse(Mssql.Log.GetErrorsAndCriticals().Any());
            Cleanup();
        }

        [TestMethod]
        public void LoadMuchParallelData()
        {
            Assert.IsNull(Mssql.DeleteTable(MethodName));

            for (int i = 0; i < 100; i++)
            {
                var csv = new Csv("Id, Data, Date, Age").AddRow(i, "Some text", DateTime.Now, new Random().Next(1, 5000));
                Assert.IsNull(Mssql.MergeCsv(csv, MethodName, "Id", false, false));
            }
            Assert.IsFalse(Mssql.Log.GetErrorsAndCriticals().Any());
            Cleanup();
        }

        /// <summary>
        /// This is not a test that fails.It only serves for testing time and thats the reason for commenting this method out.
        /// </summary>
        /// <returns></returns>
        [TestMethod]
        public void LoadMuchData2()
        {
            var csv = new Csv("id, text1, text2, number");
            for (int r = 1; r < 1000; r++)
                csv.AddRow(r, "This is a text that should indcate some length", "This is a text that should indcate some length", 105643256);

            Assert.IsNull(Mssql.DeleteTable(MethodName));
            Assert.IsNull(Mssql.MergeCsv(csv, MethodName, "id", false, false));
            Assert.IsNull(Mssql.DeleteTable(MethodName));
            Assert.IsNull(Mssql.InsertCsv(csv, MethodName, false, false));
            Assert.IsFalse(Mssql.Log.GetErrorsAndCriticals().Any());
            Cleanup();
        }

        [TestMethod]
        public void NullableNumbers()
        {
            var csv = new Csv("Id, Data, Date, Age").AddRow(1, "Some text", DateTime.Now, 22).AddRecord(2, 1, 2).AddRecord(2, 4, null);
            var csvTwo = new Csv("Id, Data, Date, Age, Name").AddRow(1, "Some more text", DateTime.Now, 22, "Knud");

            Assert.IsNull(Mssql.DeleteTable(MethodName));
            Assert.IsNull(Mssql.MergeCsv(csv, MethodName, "Id", false, false));
            Assert.IsNull(Mssql.MergeCsv(csvTwo, MethodName, "Id", false, false));
            Assert.IsFalse(Mssql.Log.GetErrorsAndCriticals().Any());
            Cleanup();
        }

        [TestMethod]
        public void SaveCsvAndOverwrite()
        {
            var csv = new Csv("Id, Data, Date, Age").AddRow(1, "Some text", DateTime.Now, 22);
            var csvTwo = new Csv("Id, Data, Date, Age, Name").AddRow(1, "Some more text", DateTime.Now, 22, "Knud");

            Assert.IsNull(Mssql.DeleteTable(MethodName));
            Assert.IsNull(Mssql.MergeCsv(csv, MethodName, "Id", false, false));
            Assert.IsNull(Mssql.TruncateTable(MethodName));
            Assert.IsNull(Mssql.MergeCsv(csvTwo, MethodName, "Id", false, false));
            Assert.IsFalse(Mssql.Log.GetErrorsAndCriticals().Any());
            Cleanup();
        }

        [TestMethod]
        public void SaveEmptyCsv()
        {
            var csv = new Csv();
            Mssql.DeleteTable(MethodName);
            Assert.IsNull(Mssql.MergeCsv(csv, MethodName, "Id", false, false));

            Assert.IsNull(Mssql.InsertCsv(csv, MethodName, false, false));
            Cleanup();
            Assert.IsFalse(Mssql.Log.GetErrorsAndCriticals().Any());
        }

        [TestMethod]
        public void SetUpdateWithoutPrimaryKey()
        {
            var csv = new Csv("Id, Data, Date, Age").AddRow(1, "Some text", DateTime.Now, 22);
            var csvTwo = new Csv("Id, Data, Date, Age, Name").AddRow(1, "Some more text", DateTime.Now, 22, "Knud");

            Mssql.DeleteTable(MethodName);

            //Raise an error because primaryKey must not be null in this method
            var errors = Mssql.MergeCsv(csv, MethodName, null, false, false);
            Assert.IsTrue(errors.Length == 1);

            errors = Mssql.InsertCsv(csv, MethodName, false, false);
            Assert.IsTrue(errors == null);
            var csvFromReader1 = Mssql.GetAsCsv(MethodName);
            Assert.IsTrue(csvFromReader1.GetRecord(1, 2).Equals("Some text"));

            Assert.IsNull(Mssql.InsertCsv(csvTwo, MethodName, false, false));
            var csvFromReader2 = Mssql.GetAsCsv(MethodName);
            Assert.IsTrue(csvFromReader2.GetRecord(2, 2).Equals("Some more text"));
            Cleanup();
        }

        [TestMethod]
        public void SetUpdateWithPrimaryKey()
        {
            var csvOne = new Csv("Id, Data, Date, Age").AddRow(1, "Some text", DateTime.Now, 22);
            var csvTwo = new Csv("Id, Data, Date, Age, Name").AddRow(1, "Some more text", DateTime.Now, 22, "Knud");

            Assert.IsNull(Mssql.DeleteTable(MethodName));
            Assert.IsNull(Mssql.MergeCsv(csvOne, MethodName, "Id", false, false));

            var csvFromReader1 = Mssql.GetAsCsv(MethodName);
            Assert.IsTrue(csvFromReader1.GetRecord(1, 2).Equals("Some text"));

            Assert.IsNull(Mssql.MergeCsv(csvTwo, MethodName, "Id", false, false));

            var csvFromReader2 = Mssql.GetAsCsv(MethodName);
            Assert.IsTrue(csvFromReader2.GetRecord(1, 2).Equals("Some more text"));
            Assert.IsTrue(csvFromReader2.GetRecord(1, 5).Equals("Knud"));
            Cleanup();
        }

        

        //Is not working as expected.... It cannot add 1 and then change to decimal 1.2 in same method 
        //[TestMethod]
        //public void ChangeTypes()
        //{
        //    var table = nameof(ChangeTypes);
        //    Assert.IsNull(mssql.DeleteTable(table));

        //    var csv = new Csv("Int, Bit").AddRow(1, true);
        //    Assert.IsNull(mssql.InserCsv(csv, table, false, true));
        //    mssql.Dispose();
        //    app = new();

        //    DbFieldCache.Flush(); // Remove all the cached DbField
        //    FieldCache.Flush(); // Remove all the cached DbField
        //    IdentityCache.Flush(); // Remove all the cached DbField
        //    PrimaryCache.Flush(); // Remove all the cached DbField

        //    var csv2 = new Csv("Int, Bit").AddRow(2.23554568, true);
        //    Assert.IsNull(mssql.InserCsv(csv2, table, false, true));

        //    var columnTypes = mssql.GetColumnTypes(table);
        //    var csvFromReader = mssql.GetAsCsv(table);


        //    //Assert.IsFalse(csvFromReader.TryGetColId("EmptyColumn", out int _));
        //    mssql.Dispose();
        //}
    }
}