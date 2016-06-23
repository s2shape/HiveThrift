using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Hive2;
using Thrift.Transport;
using System.Collections.Generic;
using System.Dynamic;

namespace HiveThrift.Testing
{
    [TestClass]
    public class Integrate
    {
        [TestMethod]
        public void TestMethod1()
        {
            string sql = "select 1 from session limit 2000";
            using (var dbConnection = new Connection(new TSocket("10.200.40.230", 21050)))
            {
                using (Cursor cursor = dbConnection.GetCursor())
                {
                    //set query pool or queue name
                    cursor.Execute("set REQUEST_POOL='hadoop-wd'");
                    //set memory size per query per node,total mem=mem_limit* count of nodes
                    cursor.Execute("set MEM_LIMIT=20gb");
                    //切换到对应的数据库
                    cursor.Execute($"use wdtest");
                    cursor.Execute(sql);
                    List<ExpandoObject> data = cursor.FetchMany(1026);
                    int sb = data.Count;
                }
            }
        }
    }
}
//case 1000 1024 2000 returns 2000
//case 1500 1024 2000 returns 2000
//case 3000 1024 2000 returns 2000 √
//case 500    1024 800 returns 800 
//case 900    1024 800 returns 800 √