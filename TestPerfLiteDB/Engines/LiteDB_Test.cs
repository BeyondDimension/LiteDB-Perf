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
using LiteDB.Engine;

namespace TestPerfLiteDB
{
    public class LiteDB_Test : ITest
    {
        private string _filename;
        private LiteEngine _db;
        private int _count;

        public int Count { get { return _count; } }
        public int FileLength { get { return (int)new FileInfo(_filename).Length; } }

        public LiteDB_Test(int count/*, string password, LiteDB.FileOptions options*/)
        {
            _count = count;
            _filename = "dblite-" + Guid.NewGuid().ToString("n") + ".db";

            //var disk = new FileDiskService(_filename, options);

            //_db = new LiteEngine(disk, password);
            _db = new LiteEngine(_filename);
        }

        public void Prepare()
        {
        }

        public void Insert()
        {
            _db.Insert("col", Helper.GetDocs(_count), default);
        }

        public void Bulk()
        {
            _db.Insert("col_bulk", Helper.GetDocs(_count), default);
        }

        public void Update()
        {
            _db.Update("col", Helper.GetDocs(_count));
        }

        public void CreateIndex()
        {
            _db.EnsureIndex("col", "name", BsonExpression.Root, false);
        }

        public void Query()
        {
            for (var i = 0; i < _count; i++)
            {
                _db.Query("col", new LiteDB.Query
                {
                    Select = LiteDB.Query.EQ("_id", i),
                }).FirstOrDefault();
            }
        }

        public void Delete()
        {
            //_db.DeleteMany("col", LiteDB.Query.All().Select);
        }

        public void Drop()
        {
            _db.DropCollection("col_bulk");
        }

        public void Dispose()
        {
            _db.Dispose();
            File.Delete(_filename);
        }
    }
}
