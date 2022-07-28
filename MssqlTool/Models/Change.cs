namespace Bygdrift.Tools.MssqlTool.Models
{

    /// <summary>What change direction a columntype has</summary>
    public enum Change
    {
        /// <summary>Not change</summary>
        None,
        /// <summary>Change from ex int to string</summary>
        Upgrade,
        /// <summary>Change from ex string to int</summary>
        Downgrade,
        /// <summary>Change from ex int to int</summary>
        Equal,
        /// <summary>Add from ex null to int</summary>
        Add,
    }
}
