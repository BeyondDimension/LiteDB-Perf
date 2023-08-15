using System;
using System.Collections.Generic;
using System.Data;
#if NETCOREAPP
using Microsoft.Data.Sqlite;
#else
using System.Data.SQLite;
#endif
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using LiteDB;

namespace TestPerfLiteDB
{
    public interface ITest : IDisposable
    {
        int Count { get; }
        int FileLength { get; }

        void Prepare();
        void Insert();
        void Bulk();
        void Update();
        void CreateIndex();
        void Query();
        void Delete();
        void Drop();
    }
}
