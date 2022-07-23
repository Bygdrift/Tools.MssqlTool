using Bygdrift.Tools.LogTool;
using System.Collections.Generic;

namespace Bygdrift.Tools.MssqlTool.Models
{
    public class Errors
    {
        /// <summary>
        /// Info about excecution
        /// </summary>
        public Log Log { get; }


        public Errors(Log log)
        {
            Log = log;
            ErrorList = new();
        }

        private List<string> ErrorList { get; set; }

        /// <summary>
        /// Return null if no errors or else an array of errors
        /// </summary>
        public string[] GetErrors
        {
            get { return ErrorList.Count == 0 ? null : ErrorList.ToArray(); }
        }

        /// <returns>All accumulated errors</returns>
        public string[] AddErrors(string newError)
        {
            ErrorList.Add(newError);
            Log.LogError(newError);
            return ErrorList.ToArray();
        }

        /// <returns>All accumulated errors</returns>
        public string[] AddErrors(string[] newErrors)
        {
            foreach (var item in newErrors)
            {
                ErrorList.Add(item);
                Log.LogError(item);
            }
            return ErrorList.ToArray();
        }
    }
}
