using Bygdrift.Tools.CsvTool;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Bygdrift.Tools.MssqlTool.Models
{
    /// <summary></summary>
    public class ColumnType
    {
        private ChangePrimaryKey? _changedPrimaryKey = null;
        private Change? _columnChange = null;
        private bool? _changedType = null;
        private string _typeExpression = null;
        //private string _typeExpressionSql = null;

        /// <summary></summary>
        public ColumnType(string name) => Name = name;

        /// <summary> </summary>
        public bool IsPrimaryKeyCsv { get; private set; }

        /// <summary> </summary>
        public bool IsPrimaryKeySql { get; private set; }

        /// <summary> </summary>
        public bool IsSetForCsv { get; set; }

        /// <summary> </summary>
        public bool IsSetForSql { get; internal set; }

        /// <summary> </summary>
        public int MaxLengthCsv { get; set; }

        /// <summary>
        /// The max length
        /// </summary>
        public int? MaxLengthSql { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string ConstraintSql { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool IsNullableSql { get; set; }

        /// <summary>
        /// The name of the column
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// like 'varchar' of 'varchar(8) 
        /// </summary>
        public SqlType? TypeNameCsv { get; set; }

        /// <summary>
        /// like 'varchar' of 'varchar(8) 
        /// </summary>
        public SqlType? TypeNameSql { get; set; }

        /// <summary>
        /// If there are changes between csv and SQL, this typeExpresion will decribe the change
        /// </summary>
        public string TypeExpression
        {
            get { return IsSetForCsv ? _typeExpression ??= TypeNameCsv + GetTypeExtension((SqlType)TypeNameCsv, MaxLengthCsv) : null; }
        }

        /// <summary>
        /// If there are any changes between sql and csv
        /// </summary>
        public bool ChangedType
        {
            get
            {
                if (_changedType == null)
                {
                    var typeExpressionSql = IsSetForSql ? TypeNameSql + GetTypeExtension((SqlType)TypeNameSql, MaxLengthSql) : null;
                    _changedType = TypeExpression != null ? (!TypeExpression.Equals(typeExpressionSql)) : true;
                }

                return (bool)_changedType;
            }
        }

        /// <summary>
        /// If there are any changes between sql and csv
        /// </summary>
        public ChangePrimaryKey ChangedPrimaryKey
        {
            get { 
                if(_changedPrimaryKey == null)
                {
                    if (IsPrimaryKeyCsv && !IsPrimaryKeySql)
                        _changedPrimaryKey = ChangePrimaryKey.Add;
                    else if (!IsPrimaryKeyCsv && IsPrimaryKeySql)
                        _changedPrimaryKey = ChangePrimaryKey.Remove;
                    else
                        _changedPrimaryKey = ChangePrimaryKey.None;
                }
                return (ChangePrimaryKey)_changedPrimaryKey;
            }
        }

        /// <summary>
        /// Add a ColumnType from the database
        /// </summary>
        public ColumnType AddSql(SqlType sqlTypeName, int? sqlMaxLength, string sqlConstraint, bool sqlIsNullable, bool sqlIsPrimaryKey = false)
        {
            IsSetForSql = true;
            TypeNameSql = sqlTypeName; // GetSqlTypeName(sqlTypeName);  //Must be set before MaxLength
            MaxLengthSql = sqlMaxLength;
            IsPrimaryKeySql = sqlIsPrimaryKey;
            ConstraintSql = sqlConstraint;
            IsNullableSql = sqlIsNullable;
            _changedPrimaryKey = null;
            _columnChange = null;
            _changedType = null;
            _typeExpression = null;
            return this;
        }

        /// <summary>
        /// Add the columntype from the CSV
        /// </summary>
        public ColumnType AddCsv(Type csvType, int csvMaxLength, bool csvIsPrimaryKey = false)
        {
            IsSetForCsv = true;
            TypeNameCsv = GetCsvToSqlTypeName(csvType);
            MaxLengthCsv = csvMaxLength;
            IsPrimaryKeyCsv = csvIsPrimaryKey;
            _changedPrimaryKey = null;
            _columnChange = null;
            _changedType = null;
            _typeExpression = null;
            return this;
        }

        /// <summary>
        /// Adds a whole csv to a list of columnTypes
        /// </summary>
        public static void AddCsv(Csv csv, string csvPrimaryKey, List<ColumnType> columnTypes)
        {
            foreach (var colType in columnTypes)
                if (csv.TryGetColId(colType.Name, out int csvColId, false))  //Not caseSensitive because SQL are not
                {
                    var csvIsPrimaryKey = csvPrimaryKey != null && csvPrimaryKey.Equals(colType.Name);
                    colType.AddCsv(csv.ColTypes[csvColId], csv.ColMaxLengths[csvColId], csvIsPrimaryKey);
                }

            var notInSqlHeaders = csv.Headers.Values.Except(columnTypes.Select(o => o.Name));
            foreach (var name in notInSqlHeaders)
            {
                if (csv.TryGetColId(name, out int csvColId, false) && csv.ColTypes.Any() && csv.ColMaxLengths.Any())  //Not caseSensitive because SQL are not
                {
                    var isPrimaryKey = csvPrimaryKey != null && csvPrimaryKey.Equals(name);
                    columnTypes.Add(new ColumnType(name).AddCsv(csv.ColTypes[csvColId], csv.ColMaxLengths[csvColId], isPrimaryKey));
                }
            }
        }

        /// <summary>
        /// Converts a string to a SQLType
        /// </summary>
        public static SqlType SqlTypeName(string type)
        {
            if (Enum.TryParse(typeof(SqlType), type.ToLower(), out object res))
                return (SqlType)res;

            throw new NotImplementedException();
        }

        /// <summary>
        /// The change between sql and csv
        /// </summary>
        public Change Change { get { return _columnChange ??= GetChange(); } }

        private Change GetChange()
        {
            if (!ChangedType)
                return Change.None;

            if (TypeNameSql == null)
                return Change.Add;

            if (TypeNameCsv == null)
                return Change.None;

            if (TypeNameSql == SqlType.bit)
            {
                if (TypeNameCsv == SqlType.bit) return Change.Equal;
                if (TypeNameCsv == SqlType.binary) return Change.Upgrade;
                if (TypeNameCsv == SqlType.smallint) return Change.Upgrade;
                if (TypeNameCsv == SqlType.@int) return Change.Upgrade;
                if (TypeNameCsv == SqlType.bigint) return Change.Upgrade;
                if (TypeNameCsv == SqlType.real) return Change.Upgrade;
                if (TypeNameCsv == SqlType.@float) return Change.Upgrade;
                if (TypeNameCsv == SqlType.@decimal) return Change.Upgrade;
                if (TypeNameCsv == SqlType.varchar) return Change.Upgrade;
            }
            if (TypeNameSql == SqlType.smallint)
            {
                if (TypeNameCsv == SqlType.bit) return Change.Downgrade;
                if (TypeNameCsv == SqlType.binary) return Change.Downgrade;
                if (TypeNameCsv == SqlType.smallint) return Change.Equal;
                if (TypeNameCsv == SqlType.@int) return Change.Upgrade;
                if (TypeNameCsv == SqlType.bigint) return Change.Upgrade;
                if (TypeNameCsv == SqlType.real) return Change.Upgrade;
                if (TypeNameCsv == SqlType.@float) return Change.Upgrade;
                if (TypeNameCsv == SqlType.@decimal) return Change.Upgrade;
                if (TypeNameCsv == SqlType.varchar) return Change.Upgrade;
            }
            if (TypeNameSql == SqlType.@int)
            {
                if (TypeNameCsv == SqlType.bit) return Change.Downgrade;
                if (TypeNameCsv == SqlType.binary) return Change.Downgrade;
                if (TypeNameCsv == SqlType.smallint) return Change.Downgrade;
                if (TypeNameCsv == SqlType.@int) return Change.Equal;
                if (TypeNameCsv == SqlType.bigint) return Change.Upgrade;
                if (TypeNameCsv == SqlType.real) return Change.Upgrade;
                if (TypeNameCsv == SqlType.@float) return Change.Upgrade;
                if (TypeNameCsv == SqlType.@decimal) return Change.Upgrade;
                if (TypeNameCsv == SqlType.varchar) return Change.Upgrade;
            }
            if (TypeNameSql == SqlType.bigint)
            {
                if (TypeNameCsv == SqlType.bit) return Change.Downgrade;
                if (TypeNameCsv == SqlType.binary) return Change.Downgrade;
                if (TypeNameCsv == SqlType.smallint) return Change.Downgrade;
                if (TypeNameCsv == SqlType.@int) return Change.Downgrade;
                if (TypeNameCsv == SqlType.bigint) return Change.Equal;
                if (TypeNameCsv == SqlType.real) return Change.Upgrade;
                if (TypeNameCsv == SqlType.@float) return Change.Upgrade;
                if (TypeNameCsv == SqlType.@decimal) return Change.Upgrade;
                if (TypeNameCsv == SqlType.varchar) return Change.Upgrade;
            }
            if (TypeNameSql == SqlType.real)
            {
                if (TypeNameCsv == SqlType.bit) return Change.Downgrade;
                if (TypeNameCsv == SqlType.binary) return Change.Downgrade;
                if (TypeNameCsv == SqlType.smallint) return Change.Downgrade;
                if (TypeNameCsv == SqlType.@int) return Change.Downgrade;
                if (TypeNameCsv == SqlType.bigint) return Change.Downgrade;
                if (TypeNameCsv == SqlType.real) return Change.Equal;
                if (TypeNameCsv == SqlType.@float) return Change.Upgrade;
                if (TypeNameCsv == SqlType.@decimal) return Change.Upgrade;
                if (TypeNameCsv == SqlType.varchar) return Change.Upgrade;
            }
            if (TypeNameSql == SqlType.@float)
            {
                if (TypeNameCsv == SqlType.bit) return Change.Downgrade;
                if (TypeNameCsv == SqlType.binary) return Change.Downgrade;
                if (TypeNameCsv == SqlType.smallint) return Change.Downgrade;
                if (TypeNameCsv == SqlType.@int) return Change.Downgrade;
                if (TypeNameCsv == SqlType.bigint) return Change.Downgrade;
                if (TypeNameCsv == SqlType.real) return Change.Downgrade;
                if (TypeNameCsv == SqlType.@float) return Change.Equal;
                if (TypeNameCsv == SqlType.@decimal) return Change.Upgrade;
                if (TypeNameCsv == SqlType.varchar) return Change.Upgrade;
            }
            if (TypeNameSql == SqlType.@decimal)
            {
                if (TypeNameCsv == SqlType.bit) return Change.Downgrade;
                if (TypeNameCsv == SqlType.binary) return Change.Downgrade;
                if (TypeNameCsv == SqlType.smallint) return Change.Downgrade;
                if (TypeNameCsv == SqlType.@int) return Change.Downgrade;
                if (TypeNameCsv == SqlType.bigint) return Change.Downgrade;
                if (TypeNameCsv == SqlType.real) return Change.Downgrade;
                if (TypeNameCsv == SqlType.@float) return Change.Downgrade;
                if (TypeNameCsv == SqlType.@decimal) return Change.Equal;
                if (TypeNameCsv == SqlType.varchar) return Change.Upgrade;
            }
            if (TypeNameSql == SqlType.datetime)
            {
                if (TypeNameCsv == SqlType.datetime) return Change.Equal;
            }
            if (TypeNameSql == SqlType.varchar)
            {
                if (TypeNameCsv == SqlType.bit) return Change.Downgrade;
                if (TypeNameCsv == SqlType.binary) return Change.Downgrade;
                if (TypeNameCsv == SqlType.smallint) return Change.Downgrade;
                if (TypeNameCsv == SqlType.@int) return Change.Downgrade;
                if (TypeNameCsv == SqlType.bigint) return Change.Downgrade;
                if (TypeNameCsv == SqlType.real) return Change.Downgrade;
                if (TypeNameCsv == SqlType.@float) return Change.Downgrade;
                if (TypeNameCsv == SqlType.@decimal) return Change.Downgrade;
                if (TypeNameCsv == SqlType.varchar) return Change.Equal;
            }
            throw new NotImplementedException();
        }

        internal static string GetTypeExtension(SqlType type, int? maxLength)
        {
            return type switch
            {
                SqlType.@decimal => "(18,12)",
                SqlType.varchar => maxLength > 8000 ? $"(MAX)" : $"({(maxLength == 0 ? 1 : maxLength)})",
                _ => ""
            };
        }

        private static SqlType GetCsvToSqlTypeName(Type type)
        {
            return Type.GetTypeCode(type) switch  //Remember to keep theses returns lowercase
            {
                TypeCode.Int64 => SqlType.bigint,
                TypeCode.Object => SqlType.varchar,
                TypeCode.Boolean => SqlType.bit,
                TypeCode.Char => SqlType.varchar,
                TypeCode.SByte => SqlType.binary,
                TypeCode.Byte => SqlType.binary,
                TypeCode.Int16 => SqlType.smallint,
                TypeCode.UInt16 => SqlType.smallint,
                TypeCode.Int32 => SqlType.@int,
                TypeCode.UInt32 => SqlType.@int,
                TypeCode.UInt64 => SqlType.bigint,
                TypeCode.Single => SqlType.real,
                TypeCode.Double => SqlType.@float,
                TypeCode.Decimal => SqlType.@decimal,
                TypeCode.DateTime => SqlType.datetime,
                TypeCode.String => SqlType.varchar,
                TypeCode.Empty => SqlType.varchar,
                TypeCode.DBNull => SqlType.varchar,
                _ => throw new NotImplementedException()
            };
        }
    }
}
