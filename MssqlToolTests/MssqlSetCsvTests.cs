using Bygdrift.Tools.CsvTool;
using Bygdrift.Tools.MssqlTool;
using Bygdrift.Tools.MssqlTool.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb;
using System;
using System.IO;
using System.Linq;

namespace MssqlToolTests
{
    [TestClass]
    public class MssqlSetTests : BaseTests
    {
        [TestMethod]
        public void PrimaryKey_Added()
        {
            Assert.IsNull(Mssql.InsertCsv(new Csv("Id").AddRow(1), MethodName));
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id").AddRow(1), MethodName, "Id"));
        }

        [TestMethod]
        public void PrimaryKey_AddedToColumnContainingNull()
        {
            Assert.IsNull(Mssql.InsertCsv(new Csv("Id, Data").AddRow(null, 1), MethodName));  //This gives a null value to a column that sholud be tried to convert into a primary key!
            try
            {
                Mssql.MergeCsv(new Csv("Id").AddRow(1), MethodName, "Id");
            }
            catch (Exception e)
            {
                Assert.IsTrue(e.Message.StartsWith("Error in db load: Cannot define PRIMARY KEY constraint on nullable column in table"));
            }
        }

        [TestMethod]
        public void PrimaryKey_Changed()
        {
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id1, Id2").AddRow(1, 1), MethodName, "Id1"));
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id1, Id2").AddRow(1, 1), MethodName, "Id2"));
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id1, Id2").AddRow(1, 1), MethodName, "Id1"));
        }

        [TestMethod]
        public void PrimaryKey_UpdateToVarChar()
        {
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id, Data").AddRow(1, 1), MethodName, "Id"));
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id, Data, Age").AddRow("ww", "text", 21), MethodName, "Id"));
        }

        [TestMethod]
        public void TruncateTable()
        {
            var csvOne = new Csv("Id").AddRow(1);
            var csvTwo = new Csv("Id").AddRow(2);

            Assert.IsNull(Mssql.MergeCsv(csvOne, MethodName, "Id", false));
            Assert.IsNull(Mssql.MergeCsv(csvTwo, MethodName, "Id", true));

            var csvFromReader = Mssql.GetAsCsv(MethodName);
            Assert.AreEqual(csvFromReader.RowCount, 1);
            Assert.IsFalse(Mssql.Log.GetErrorsAndCriticals().Any());
        }

        [TestMethod]
        public void FailsWithoutFlush()
        {
            var table = $"{Mssql.SchemaName}.{MethodName}";
            Mssql.Connection.ExecuteNonQuery($"CREATE TABLE {table} (Id int)");
            Mssql.Connection.Insert(table, new { Id = 1 });
            Mssql.Connection.ExecuteNonQuery($"ALTER TABLE {table} ALTER COLUMN Id VARCHAR(1)");
            Mssql.FlushRepoDb();  //This line is necesary
            Mssql.Connection.Insert(table, new { Id = "A" });
        }

        [TestMethod]
        public void ChangeColumnType()
        {
            var log2 = Mssql.InsertCsv(new Csv(), MethodName);
            var log = Mssql.InsertCsv(new Csv("Id, Data").AddRow(1, 1), MethodName);
            Assert.IsNull(log);
            Assert.AreEqual(Mssql.GetColumnTypes(MethodName).Last().TypeNameSql, SqlType.@int);

            log = Mssql.InsertCsv(new Csv("Id, Data").AddRow(1, "T"), MethodName);
            Assert.IsNull(log);
            Assert.AreEqual(Mssql.GetColumnTypes(MethodName).Last().TypeNameSql, SqlType.varchar);

            log = Mssql.InsertCsv(new Csv("Id, Data").AddRow(1, 1), MethodName, true);  //When truncating, the whole table is deleted, so the column Data is change
            Assert.IsNull(log);
            Assert.AreEqual(Mssql.GetColumnTypes(MethodName).Last().TypeNameSql, SqlType.@int);
        }

        [TestMethod]
        public void ChangeColumnTypeFromVarCharToInt()
        {
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id, Data").AddRow(1, "A"), MethodName, "Id"));
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id, Data").AddRow(1, 1), MethodName, "Id", false, true));
            Assert.AreEqual(Mssql.GetColumnTypes(MethodName).Last().TypeNameSql, SqlType.@int);
        }

        [TestMethod]
        public void NormalColumnUpdatedToPrimaryKey()
        {
            Assert.IsNull(Mssql.InsertCsv(new Csv("Id").AddRow("w"), MethodName));
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id").AddRow("ww"), MethodName, "Id"));
        }

        [TestMethod]
        public void PrimaryKey_Removed()
        {
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id").AddRow(1), MethodName, "Id"));
            Assert.IsNull(Mssql.InsertCsv(new Csv("Id").AddRow(1), MethodName));
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

            Assert.IsNull(Mssql.MergeCsv(csvOne, MethodName, "Id"));
            Assert.IsNull(Mssql.MergeCsv(csvTwo, MethodName, "Id"));

            var csvFromReader = Mssql.GetAsCsv(MethodName);
            Assert.AreEqual(csvFromReader.GetRecord(1, 2), "Some more text");
            Assert.AreEqual(csvFromReader.GetRecord(1, 5), "Knud");
        }

        [TestMethod]
        public void ChangeIntToVarchar()
        {
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id, Data").AddRow("A", 5), MethodName, "Id"));
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id, Data").AddRow("A", "AB"), MethodName, "Id"));
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id, Data").AddRow("A", 6), MethodName, "Id"));
            var columns = Mssql.GetColumnTypes(MethodName).ToList();
            Assert.AreEqual(columns[1].TypeNameSql, SqlType.varchar);

            Assert.IsNull(Mssql.DeleteTable(MethodName));
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id, Data").AddRow("A", "AB"), MethodName, "Id"));
            Assert.IsNull(Mssql.MergeCsv(new Csv("Id, Data").AddRow("A", 6), MethodName, "Id"));
            columns = Mssql.GetColumnTypes(MethodName).ToList();
            Assert.AreEqual(columns[1].TypeNameSql, SqlType.varchar);
        }

        [TestMethod]
        public void LoadMuchData()
        {
            var path = Path.Combine(BasePath, "Files", "Csv", "Height and weight for 25000 persons.csv");
            var csv = new Csv().FromCsvFile(path);
            Assert.IsNull(Mssql.MergeCsv(csv, MethodName, "Index"));
            Assert.IsFalse(Mssql.Log.GetErrorsAndCriticals().Any());
        }

        [TestMethod]
        public void LoadMuchParallelData()
        {
            for (int i = 0; i < 100; i++)
            {
                var csv = new Csv("Id, Data, Date, Age").AddRow(i, "Some text", DateTime.Now, new Random().Next(1, 5000));
                Assert.IsNull(Mssql.MergeCsv(csv, MethodName, "Id"));
            }
            Assert.IsFalse(Mssql.Log.GetErrorsAndCriticals().Any());
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

            Assert.IsNull(Mssql.MergeCsv(csv, MethodName, "id"));
            Assert.IsNull(Mssql.DeleteTable(MethodName));
            Assert.IsNull(Mssql.InsertCsv(csv, MethodName));
            Assert.IsFalse(Mssql.Log.GetErrorsAndCriticals().Any());
        }

        [TestMethod]
        public void NullableNumbers()
        {
            var csv = new Csv("Id, Data, Date, Age").AddRow(1, "Some text", DateTime.Now, 22).AddRecord(2, 1, 2).AddRecord(2, 4, null);
            var csvTwo = new Csv("Id, Data, Date, Age, Name").AddRow(1, "Some more text", DateTime.Now, 22, "Knud");
            Assert.IsNull(Mssql.MergeCsv(csv, MethodName, "Id"));
            Assert.IsNull(Mssql.MergeCsv(csvTwo, MethodName, "Id"));
            Assert.IsFalse(Mssql.Log.GetErrorsAndCriticals().Any());
        }

        [TestMethod]
        public void SaveCsvAndOverwrite()
        {
            var csv = new Csv("Id, Data, Date, Age").AddRow(1, "Some text", DateTime.Now, 22);
            var csvTwo = new Csv("Id, Data, Date, Age, Name").AddRow(1, "Some more text", DateTime.Now, 22, "Knud");
            Assert.IsNull(Mssql.MergeCsv(csv, MethodName, "Id"));
            Assert.IsNull(Mssql.TruncateTable(MethodName));
            Assert.IsNull(Mssql.MergeCsv(csvTwo, MethodName, "Id"));
            Assert.IsFalse(Mssql.Log.GetErrorsAndCriticals().Any());
        }

        [TestMethod]
        public void SaveEmptyCsv()
        {
            var csv = new Csv();
            Assert.IsNull(Mssql.MergeCsv(csv, MethodName, "Id"));
            Assert.IsNull(Mssql.InsertCsv(csv, MethodName));
        }

        [TestMethod]
        public void SetUpdateWithoutPrimaryKey()
        {
            var csv = new Csv("Id, Data, Date, Age").AddRow(1, "Some text", DateTime.Now, 22);
            var csvTwo = new Csv("Id, Data, Date, Age, Name").AddRow(1, "Some more text", DateTime.Now, 22, "Knud");

            //Raise an error because primaryKey must not be null in this method
            var errors = Mssql.MergeCsv(csv, MethodName, null);
            Assert.IsTrue(errors.Length == 1);

            errors = Mssql.InsertCsv(csv, MethodName);
            Assert.IsTrue(errors == null);
            var csvFromReader1 = Mssql.GetAsCsv(MethodName);
            Assert.IsTrue(csvFromReader1.GetRecord(1, 2).Equals("Some text"));

            Assert.IsNull(Mssql.InsertCsv(csvTwo, MethodName));
            var csvFromReader2 = Mssql.GetAsCsv(MethodName);
            Assert.IsTrue(csvFromReader2.GetRecord(2, 2).Equals("Some more text"));
        }

        [TestMethod]
        public void SetUpdateWithPrimaryKey()
        {
            var csvOne = new Csv("Id, Data, Date, Age").AddRow(1, "Some text", DateTime.Now, 22);
            var csvTwo = new Csv("Id, Data, Date, Age, Name").AddRow(1, "Some more text", DateTime.Now, 22, "Knud");

            Assert.IsNull(Mssql.MergeCsv(csvOne, MethodName, "Id"));

            var csvFromReader1 = Mssql.GetAsCsv(MethodName);
            Assert.IsTrue(csvFromReader1.GetRecord(1, 2).Equals("Some text"));

            Assert.IsNull(Mssql.MergeCsv(csvTwo, MethodName, "Id"));

            var csvFromReader2 = Mssql.GetAsCsv(MethodName);
            Assert.IsTrue(csvFromReader2.GetRecord(1, 2).Equals("Some more text"));
            Assert.IsTrue(csvFromReader2.GetRecord(1, 5).Equals("Knud"));
        }
    }
}