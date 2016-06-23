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
            string sql = "select 1 from xxxx limit 3000";
            using (var dbConnection = new Connection(new TSocket("xxxx", 1234)))
            {
                using (Cursor cursor = dbConnection.GetCursor())
                {
                    //切换到对应的数据库
                    cursor.Execute($"use xxxx");
                    cursor.Execute(sql);
                    List<ExpandoObject> data = cursor.FetchMany(1);
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