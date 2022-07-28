namespace Bygdrift.Tools.MssqlTool.Models
{

    /// <summary>What change direction a columntype has</summary>
    public enum ChangePrimaryKey
    {
        /// <summary>Not change</summary>
        None,
        /// <summary>Add from ex null to int</summary>
        Add,
        /// <summary>Add from ex int to null</summary>
        Remove,
    }
}
