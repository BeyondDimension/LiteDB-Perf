using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;

namespace TestPerfLiteDB
{
    public class SQLite_ORM_Test : ITest
    {
        private string _filename;
        private SQLiteDbContext _db;
        private int _count;

        public int Count { get { return _count; } }
        public int FileLength { get { return (int)new FileInfo(_filename).Length; } }

        public SQLite_ORM_Test(int count, string password, bool journal)
        {
            _count = count;
            _filename = "sqlite-" + Guid.NewGuid().ToString("n") + ".db";
            var cs = "Data Source=" + _filename;
            if (password != null) cs += "; Password=" + password;
            if (journal == false) cs += "; Journal Mode=Off";
            _db = new SQLiteDbContext(cs);
        }

        public void Prepare()
        {
            _db.Database.ExecuteSqlRaw("CREATE TABLE col (id INTEGER NOT NULL PRIMARY KEY, name TEXT, lorem TEXT)");
            _db.Database.ExecuteSqlRaw("CREATE TABLE col_bulk (id INTEGER NOT NULL PRIMARY KEY, name TEXT, lorem TEXT)");
        }

        public void Insert()
        {
            foreach (var doc in Helper.GetDocs(_count))
            {
                _db.TableCols.AddRange((TableCol)doc);
            }
            _db.SaveChanges();
        }

        public void Bulk()
        {
            _db.TableColBulks.AddRange(Helper.GetDocs(_count).Select(a => (TableColBulk)a));
            _db.SaveChanges();
        }

        public void Update()
        {
            _db.ChangeTracker.Clear();
            _db.TableCols.UpdateRange(Helper.GetDocs(_count).Select(a => (TableCol)a));
            _db.SaveChanges();
        }

        public void CreateIndex()
        {
            _db.Database.ExecuteSqlRaw("CREATE INDEX idx1 ON col (name)");
        }

        public void Query()
        {
            for (var i = 0; i < _count; i++)
            {
                _db.TableCols.FirstOrDefault(a => a.Id == i);
            }
        }

        public void Delete()
        {
            _db.TableCols.ExecuteDelete();
        }

        public void Drop()
        {
            _db.Database.ExecuteSqlRaw("DROP TABLE col_bulk");
        }

        public void Dispose()
        {
            _db.Database.EnsureDeleted();
            _db.Dispose();
            File.Delete(_filename);
        }
    }

    #region DbContext & Entities

    public class SQLiteDbContext : DbContext
    {
        public DbSet<TableCol> TableCols { get; set; }
        public DbSet<TableColBulk> TableColBulks { get; set; }

        public string ConnectionString { get; }

        public SQLiteDbContext(string connection)
        {
            ConnectionString = connection;
        }

        protected override void OnConfiguring(DbContextOptionsBuilder options)
            => options.UseSqlite(ConnectionString);
    }

    [Table("col")]
    public class TableCol
    {
        [DatabaseGenerated(DatabaseGeneratedOption.None)]
        public int Id { get; set; }

        public string? Name { get; set; }

        public string? Lorem { get; set; }

        public static implicit operator TableCol(LiteDB.BsonDocument source) =>
            new() { Id = source["_id"].AsInt32, Name = source["name"].AsString, Lorem = source["lorem"].AsString };
    }

    [Table("col_bulk")]
    public class TableColBulk
    {
        [DatabaseGenerated(DatabaseGeneratedOption.None)]
        public int Id { get; set; }

        public string? Name { get; set; }

        public string? Lorem { get; set; }

        public static implicit operator TableColBulk(LiteDB.BsonDocument source) =>
            new() { Id = source["_id"].AsInt32, Name = source["name"].AsString, Lorem = source["lorem"].AsString };
    }

    #endregion DbContext & Entities
}