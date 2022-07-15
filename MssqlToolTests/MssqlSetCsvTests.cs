using Bygdrift.Tools.CsvTool;
using Bygdrift.Tools.LogTool;
using Bygdrift.Tools.MssqlTool;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb;
using System;
using System.IO;
using System.Linq;

namespace MssqlToolTests
{
    [TestClass]
    public class MssqlSetTests
    {
        /// <summary>Path to project base</summary>
        public static readonly string BasePath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..\\..\\..\\"));
        
        readonly Mssql mssql;

        public MssqlSetTests() => mssql = new Mssql("", "Test", new Log());

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

        /// <summary>
        /// this would fail without flush fail and I asked for a solution here: https://stackoverflow.com/questions/69635893/repodb-doesnt-merge-correct-after-altering-a-column
        /// If RepoDB didn't get flushed, the reulst would be: Id = 1 Data = "Lo", Name = NULL
        /// </summary>
        [TestMethod]
        public void CreateAndTruncateTable_Fails()
        {
            var csvOne = new Csv("Id, Data, Date, Age").AddRow(1, "Some text", DateTime.Now, 22);
            var csvTwo = new Csv("Id, Data, Date, Age, Name").AddRow(1, "Some more text", DateTime.Now, 22, "Knud");
            var table = "CreateAndTruncateTableFails";

            Assert.IsNull(mssql.DeleteTable(table));
            Assert.IsNull(mssql.MergeCsv(csvOne, table, "Id", false, false));
            Assert.IsNull(mssql.MergeCsv(csvTwo, table, "Id", false, false));

            var csvFromReader = mssql.GetAsCsv(table);
            Assert.IsTrue(csvFromReader.GetRecord(1, 2).Equals("Some more text"));
            Assert.IsTrue(csvFromReader.GetRecord(1, 5).Equals("Knud"));

            Assert.IsFalse(mssql.Log.GetErrorsAndCriticals().Any());
            Assert.IsNull(Cleanup(table));
        }


        /// <summary>
        /// If there are a column left that are all empty, it should be removed
        /// </summary>
        [TestMethod]
        public void IfColumnsGetsRemoved()
        {
            var table = "IfColumnsGetsRemoved";
            var csv = new Csv("Id, Data, Date, Age, EmptyColumn").AddRow(1, "Some text", DateTime.Now, 22);

            Assert.IsNull(mssql.DeleteTable(table));
            Assert.IsNull(mssql.MergeCsv(csv, table, "Id", false, true));
            var csvFromReader = mssql.GetAsCsv(table);
            Assert.IsFalse(csvFromReader.TryGetColId("EmptyColumn", out int _));
            Assert.IsNull(Cleanup(table));
        }

        [TestMethod]
        public void ChangeIntToVarchar()
        {
            var table = "ChangeIntToVarchar";
            Assert.IsNull(mssql.DeleteTable(table));
            Assert.IsNull(mssql.MergeCsv(new Csv("Id, Data").AddRow("A", 5), table, "Id", false, false));
            Assert.IsNull(mssql.MergeCsv(new Csv("Id, Data").AddRow("A", "AB"), table, "Id", false, false));
            Assert.IsNull(mssql.MergeCsv(new Csv("Id, Data").AddRow("A", 6), table, "Id", false, false));
            Assert.IsNull(mssql.DeleteTable(table));
            Assert.IsNull(mssql.MergeCsv(new Csv("Id, Data").AddRow("A", "AB"), table, "Id", false, false));
            Assert.IsNull(mssql.MergeCsv(new Csv("Id, Data").AddRow("A", 6), table, "Id", false, false));
            Assert.IsNull(Cleanup(table));
        }

        [TestMethod]
        public void InsertCsvThenMergeFailsOnPrimaryKeay()
        {
            var table = "InsertCsvThenMergeFailsOnPrimaryKeay";
            Assert.IsNull(mssql.DeleteTable(table));
            Assert.IsNull(mssql.InserCsv(new Csv("Id, Data").AddRow(1, "Some text"), table, false, false));
            try
            {
                mssql.MergeCsv(new Csv("Id, Data").AddRow(1, "Some more text"), table, "Id", false, false);
            }
            catch (Exception)
            {
            }
            Assert.AreEqual(mssql.Log.GetErrorsAndCriticals().Count(), 2);
            Assert.IsNull(Cleanup(table));
        }

        [TestMethod]
        public void LoadMuchData()
        {
            var table = "LoadMuchData";
            var path = Path.Combine(BasePath, "Files", "Csv", "Height and weight for 25000 persons.csv");
            var csv = new Csv().FromCsvFile(path);

            Assert.IsNull(mssql.DeleteTable(table));
            Assert.IsNull(mssql.MergeCsv(csv, table, "Index", false, false));
            Assert.IsFalse(mssql.Log.GetErrorsAndCriticals().Any());
            Assert.IsNull(Cleanup(table));
        }

        [TestMethod]
        public void LoadMuchParallelData()
        {
            var table = "LoadMuchParallelData";
            Assert.IsNull(mssql.DeleteTable(table));

            for (int i = 0; i < 3; i++)
            {
                var csv = new Csv("Id, Data, Date, Age").AddRow(i, "Some text", DateTime.Now, new Random().Next(1, 5000));
                Assert.IsNull(mssql.MergeCsv(csv, table, "Id", false, false));
            }
            Assert.IsFalse(mssql.Log.GetErrorsAndCriticals().Any());
            Assert.IsNull(Cleanup(table));
        }

        /// <summary>
        /// This is not a test that fails.It only serves for testing time and thats the reason for commenting this method out.
        /// </summary>
        /// <returns></returns>
        [TestMethod]
        public void LoadMuchData2()
        {
            var table = "LoadMuchData2";
            var csv = new Csv("id, text1, text2, number");
            for (int r = 1; r < 1000; r++)
                csv.AddRow(r, "This is a text that should indcate some length", "This is a text that should indcate some length", 105643256);

            Assert.IsNull(mssql.DeleteTable(table));
            Assert.IsNull(mssql.MergeCsv(csv, table, "id", false, false));
            Assert.IsNull(mssql.DeleteTable(table));
            Assert.IsNull(mssql.InserCsv(csv, table, false, false));
            Assert.IsFalse(mssql.Log.GetErrorsAndCriticals().Any());
            Assert.IsNull(Cleanup(table));
        }

        [TestMethod]
        public void NullableNumbers()
        {
            var table = "SaveCsvAndOverwrite";
            var csv = new Csv("Id, Data, Date, Age").AddRow(1, "Some text", DateTime.Now, 22).AddRecord(2, 1, 2).AddRecord(2, 4, null);
            var csvTwo = new Csv("Id, Data, Date, Age, Name").AddRow(1, "Some more text", DateTime.Now, 22, "Knud");

            Assert.IsNull(mssql.DeleteTable(table));
            Assert.IsNull(mssql.MergeCsv(csv, table, "Id", false, false));
            Assert.IsNull(mssql.MergeCsv(csvTwo, table, "Id", false, false));
            Assert.IsFalse(mssql.Log.GetErrorsAndCriticals().Any());
            Assert.IsNull(Cleanup(table));
        }

        [TestMethod]
        public void SaveCsvAndOverwrite()
        {
            var table = "SaveCsvAndOverwrite";
            var csv = new Csv("Id, Data, Date, Age").AddRow(1, "Some text", DateTime.Now, 22);
            var csvTwo = new Csv("Id, Data, Date, Age, Name").AddRow(1, "Some more text", DateTime.Now, 22, "Knud");

            Assert.IsNull(mssql.DeleteTable(table));
            Assert.IsNull(mssql.MergeCsv(csv, table, "Id", false, false));
            Assert.IsNull(mssql.TruncateTable(table));
            Assert.IsNull(mssql.MergeCsv(csvTwo, table, "Id", false, false));
            Assert.IsFalse(mssql.Log.GetErrorsAndCriticals().Any());
            Assert.IsNull(Cleanup(table));
        }

        [TestMethod]
        public void SaveEmptyCsv()
        {
            var table = "SaveEmptyCsv";
            var csv = new Csv();

            mssql.DeleteTable(table);
            var errors = mssql.MergeCsv(csv, table, "Id", false, false);
            Assert.AreEqual(errors.Length, 1);

            Assert.IsNull(mssql.InserCsv(csv, table, false, false));
            Assert.IsTrue(mssql.Log.GetErrorsAndCriticals().Any());
            Assert.IsNull(Cleanup(table));
        }

        [TestMethod]
        public void SetUpdateWithoutPrimaryKey()
        {
            var table = "SetUpdateWithoutPrimaryKey";
            var csv = new Csv("Id, Data, Date, Age").AddRow(1, "Some text", DateTime.Now, 22);
            var csvTwo = new Csv("Id, Data, Date, Age, Name").AddRow(1, "Some more text", DateTime.Now, 22, "Knud");

            mssql.DeleteTable(table);

            //Raise an error because primaryKey must not be null in this method
            var errors = mssql.MergeCsv(csv, table, null, false, false);
            Assert.IsTrue(errors.Length == 1);

            errors = mssql.InserCsv(csv, table, false, false);
            Assert.IsTrue(errors == null);
            var csvFromReader1 = mssql.GetAsCsv(table);
            Assert.IsTrue(csvFromReader1.GetRecord(1, 2).Equals("Some text"));

            Assert.IsNull(mssql.InserCsv(csvTwo, table, false, false));
            var csvFromReader2 = mssql.GetAsCsv(table);
            Assert.IsTrue(csvFromReader2.GetRecord(2, 2).Equals("Some more text"));
            Assert.IsNull(Cleanup(table));
        }

        [TestMethod]
        public void SetUpdateWithPrimaryKey()
        {
            var table = "SetUpdateWithPrimaryKey";
            var csvOne = new Csv("Id, Data, Date, Age").AddRow(1, "Some text", DateTime.Now, 22);
            var csvTwo = new Csv("Id, Data, Date, Age, Name").AddRow(1, "Some more text", DateTime.Now, 22, "Knud");

            Assert.IsNull(mssql.DeleteTable(table));
            Assert.IsNull(mssql.MergeCsv(csvOne, table, "Id", false, false));

            var csvFromReader1 = mssql.GetAsCsv(table);
            Assert.IsTrue(csvFromReader1.GetRecord(1, 2).Equals("Some text"));

            Assert.IsNull(mssql.MergeCsv(csvTwo, table, "Id", false, false));

            var csvFromReader2 = mssql.GetAsCsv(table);
            Assert.IsTrue(csvFromReader2.GetRecord(1, 2).Equals("Some more text"));
            Assert.IsTrue(csvFromReader2.GetRecord(1, 5).Equals("Knud"));
            Assert.IsNull(Cleanup(table));
        }

        private string Cleanup(string tableName)
        {
            var res = mssql.DeleteTable(tableName);
            mssql.Dispose();
            return res;
        }
    }
}