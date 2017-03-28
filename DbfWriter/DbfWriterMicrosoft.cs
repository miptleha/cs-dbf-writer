using System;
using System.Data;
using System.Data.OleDb;
using System.IO;

namespace DbfWriter
{
    /// <summary>
    /// Write dbf via Microsoft driver
    /// This driver can create memo fields (currently not implemented, see line 66, 112)
    /// </summary>
    public class DbfWriterMicrosoft
    {
        /// <summary>
        /// Save dbf in dBASE IV format
        /// </summary>
        /// <param name="dt">table to save in file</param>
        /// <param name="path">path to the file</param>
        /// <param name="tableName">filename</param>
        public static void Write(DataTable dt, string path, string tableName)
        {
            File.Delete(path + "\\" + tableName + ".DBF");
            string conStr = string.Format("Provider=Microsoft.Jet.OLEDB.4.0;Data Source={0};Extended Properties=dBASE IV;User ID=Admin;Password=;", path);

            using (var con = new OleDbConnection(conStr))
            {
                con.Open();

                string names = "";
                string columns = "";
                foreach (DataColumn dc in dt.Columns)
                {
                    string name = dc.ColumnName;

                    if (names.Length > 0)
                    {
                        names += ", ";
                        columns += ", ";
                    }
                    names += name;

                    //calculate size of text fields
                    string size = "";
                    string type = "";
                    if (dc.DataType == typeof(int))
                    {
                        type = "int";
                    }
                    else if (dc.DataType == typeof(string))
                    {
                        type = "char";
                        int fieldSize = 0;
                        foreach (DataRow dr in dt.Rows)
                        {
                            var val = dr[dc];
                            if (val is string)
                            {
                                string strVal = (string)val;
                                int s = strVal.Trim().Length;
                                if (fieldSize < s)
                                    fieldSize = s;
                            }
                        }

                        if (fieldSize > 255)
                            type = "memo";
                        else
                            size = "(" + fieldSize + ")";
                    }
                    else if (dc.DataType == typeof(DateTime))
                    {
                        type = "date";
                    }
                    else
                    {
                        //implement here parsing of others types (decimal, long, etc). dbf types: http://dbf-software.com/create-table.html)
                        throw new NotImplementedException(string.Format("Column of type {0} not implemented", dc.DataType));
                    }

                    columns += name + " " + type + size;
                }

                IDbCommand cmdCreate = con.CreateCommand();
                cmdCreate.CommandText = string.Format("CREATE TABLE {0}({1})", tableName, columns);
                cmdCreate.ExecuteNonQuery();

                int count = 0;
                int num = 0;
                foreach (DataRow dr in dt.Rows)
                {
                    IDbCommand cmdInsert = con.CreateCommand();
                    string values = "";
                    foreach (DataColumn dc in dt.Columns)
                    {
                        object value = dr[dc];

                        if (values.Length > 0)
                            values += ", ";

                        if (value == null || value == DBNull.Value || value.ToString().Length == 0)
                        {
                            values += "NULL";
                        }
                        else if (dc.DataType == typeof(int))
                        {
                            values += value.ToString();
                        }
                        else if (dc.DataType == typeof(string))
                        {
                            num++;
                            values += "?";
                            var p = new OleDbParameter("@p" + num, OleDbType.LongVarChar);
                            p.Value = value.ToString();
                            cmdInsert.Parameters.Add(p);
                        }
                        else if (dc.DataType == typeof(DateTime))
                        {
                            num++;
                            values += "?";
                            var p = new OleDbParameter("@p" + num, OleDbType.Date);
                            p.Value = (DateTime)value;
                            cmdInsert.Parameters.Add(p);
                        }
                        else
                        {
                            throw new NotImplementedException(string.Format("Column of type {0} not implemented", dc.DataType));
                        }
                    }

                    cmdInsert.CommandText = string.Format("INSERT INTO {0}({1}) VALUES({2})", tableName, names, values);
                    cmdInsert.ExecuteNonQuery();
                    count++;
                }
            }
        }
    }
}
