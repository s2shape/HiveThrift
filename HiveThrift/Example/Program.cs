using System;
using System.Collections.Generic;
using Hive2;

namespace Example
{
    class Program
    {
        private static void Main(string[] args)
        {
            try
            {
                using (var conn = new Connection("xxx", 10000, "xx", "xx",
                    TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6))
                {
                    var cursor = conn.GetCursor();
                    cursor.Execute("use xx");
                    var list = cursor.FetchMany(100);
                    if (!list.IsEmpty())
                    {
                        var dict = list[0] as IDictionary<string, object>;
                        foreach (var key in dict.Keys)
                        {
                            Console.WriteLine(key + dict[key].ToString());
                        }
                    }
                    else
                    {
                        Console.WriteLine("no result");
                    }
                    cursor.Execute("select * from xx");
                    var list2 = cursor.FetchMany(100);
                    if (!list2.IsEmpty())
                    {
                        var dict = list2[0] as IDictionary<string, object>;
                        foreach (var key in dict.Keys)
                        {
                            Console.WriteLine(key + dict[key].ToString());
                        }
                    }
                    else
                    {
                        Console.WriteLine("no result");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            Console.ReadLine();
        }

    }
}
