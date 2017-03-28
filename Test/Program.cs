using DbfWriter;
using System;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Reflection;

namespace Test
{
    class Program
    {
        static void Main(string[] args)
        {
            DataTable dt = new DataTable();
            dt.Columns.Add("Dosage", typeof(int));
            dt.Columns.Add("Drug", typeof(string));
            dt.Columns.Add("Patient", typeof(string));
            dt.Columns.Add("Dat", typeof(DateTime));
            dt.Columns.Add("Dd", typeof(string));

            string desc = ""; //memo field
            for (int i = 0; i < 1000; i++)
                desc += i.ToString();

            for (int i = 0; i < 1000; i++)
            {
                dt.Rows.Add(25, "Indocin", "David", DateTime.Now, desc);
                dt.Rows.Add(50, "Enebrel", "Sam", DateTime.Now, desc);
                dt.Rows.Add(10, "Hydralazine", "Christoff", DateTime.Now, desc);
                dt.Rows.Add(21, "Combivent", "Janet", DateTime.Now, desc);
                dt.Rows.Add(100, "Dilantin", "Melanie", DateTime.Now, desc);
            }

            Console.WriteLine("Speed test started....");
            Console.WriteLine("Test table: columns: {0:n0}, rows: {1:n0}\n", dt.Columns.Count, dt.Rows.Count);

            string path = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

            Stopwatch sw1 = Stopwatch.StartNew();
            Console.WriteLine("Create dbf with raw writing to file (DbfWriter class): test1.dbf");
            DbfWriterFast.Write(dt, path, "test1");
            sw1.Stop();
            Console.WriteLine("File was created in " + sw1.Elapsed.ToString() + "\n");

            Stopwatch sw2 = Stopwatch.StartNew();
            Console.WriteLine("Create dbf using Microsoft driver (DbfWriterMicrosoft class): test2.dbf");
            DbfWriterMicrosoft.Write(dt, path, "test2");
            sw2.Stop();
            Console.WriteLine("File was create in " + sw2.Elapsed.ToString() + "\n");

            Console.WriteLine(string.Format("Raw method is {0:n0} times faster!", sw2.ElapsedMilliseconds / sw1.ElapsedMilliseconds));
        }
    }
}
