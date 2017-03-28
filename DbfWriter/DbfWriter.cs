using System;
using System.IO;
using System.Data;
using System.Globalization;
using System.Text;

namespace DbfWriter
{
    /// <summary>
    /// Fast dbf writing direct to file without any drivers
    /// </summary>
    public class DbfWriterFast
    {
        /// <summary>
        /// Save dbf in dBASE III 2.0 Format
        /// </summary>
        /// <param name="dt">table to save in file</param>
        /// <param name="path">folder to save in</param>
        /// <param name="tableName">filename</param>
        /// <param name="dbfEncoding">codepage of text data (each char saved as byte), default - 866 (MS-DOS Russian)</param>
        public static void Write(DataTable dt, string path, string tableName, int dbfEncoding = 866)
        {
            //calculate size of text columns
            int[] fieldSize = new int[dt.Columns.Count];
            foreach (DataRow dr in dt.Rows)
            {
                for (int i = 0; i < dt.Columns.Count; i++)
                {
                    var c = dt.Columns[i];
                    var val = dr[i];
                    if (val is string)
                    {
                        string strVal = (string)val;
                        int size = strVal.Trim().Length;
                        if (fieldSize[i] < size)
                            fieldSize[i] = size;
                    }
                }
            }

            //write header
            var data = DataTableSaveToDBFStart(dt, path, tableName, fieldSize);

            //write table
            foreach (DataRow dr in dt.Rows)
            {
                DataRowSaveToDBF(dr, data.FS, data.FieldType, data.FieldSize, dbfEncoding);
            }

            //write tail
            DataTableSaveToDBFEnd(data.FS);
        }

        /// <summary>
        /// Write header
        /// </summary>
        /// <param name="DT">source table</param>
        /// <param name="Folder">folder to save dbf</param>
        /// <param name="tableName">filename</param>
        /// <param name="fieldSize">size of columns</param>
        /// <returns>data for DataRowSaveToDBF method</returns>
        private static DataDbf DataTableSaveToDBFStart(DataTable DT, string Folder, string tableName, int[] fieldSize)
        {
            //create file
            File.Delete(Folder + "\\" + tableName + ".DBF");
            FileStream FS = new FileStream(Folder + "\\" + tableName + ".DBF", FileMode.Create);
            
            //dBASE III 2.0 Format
            byte[] buffer = new byte[] { 0x03, 0x63, 0x04, 0x04 }; // Header 4 bytes
            FS.Write(buffer, 0, buffer.Length);
            buffer = new byte[]{
                (byte)(((DT.Rows.Count % 0x1000000) % 0x10000) % 0x100),
                (byte)(((DT.Rows.Count % 0x1000000) % 0x10000) / 0x100),
                (byte)(( DT.Rows.Count % 0x1000000) / 0x10000),
                (byte)( DT.Rows.Count / 0x1000000)
            }; // Word32 -> number of rows 5-8 bytes
            FS.Write(buffer, 0, buffer.Length);
            int i = (DT.Columns.Count + 1) * 32 + 1;
            buffer = new byte[]{
                (byte)( i % 0x100),
                (byte)( i / 0x100)
            }; // Word16 -> number of columns 9-10 bytes
            FS.Write(buffer, 0, buffer.Length);
            string[] FieldName = new string[DT.Columns.Count]; // array: name of fields
            string[] FieldType = new string[DT.Columns.Count]; // array: type of fields
            byte[] FieldSize = new byte[DT.Columns.Count]; // array: size of fields
            byte[] FieldDigs = new byte[DT.Columns.Count]; // array: size of fraction
            int s = 1; // size of header
            foreach (DataColumn C in DT.Columns)
            {
                string l = C.ColumnName.ToUpper(); // column name
                while (l.Length < 10) { l = l + (char)0; } // aligh (10 bytes)
                FieldName[C.Ordinal] = l.Substring(0, 10) + (char)0; // result
                FieldType[C.Ordinal] = "C";
                FieldSize[C.Ordinal] = 50;
                FieldDigs[C.Ordinal] = 0;
                switch (C.DataType.ToString())
                {
                    case "System.String":
                        {
                            FieldSize[C.Ordinal] = fieldSize[C.Ordinal] > 255 ? (byte)255 : (byte)fieldSize[C.Ordinal];
                            if (FieldSize[C.Ordinal] == 0)
                                FieldSize[C.Ordinal] = 1;
                            break;
                        }
                    case "System.Boolean": { FieldType[C.Ordinal] = "L"; FieldSize[C.Ordinal] = 1; break; }
                    case "System.Byte": { FieldType[C.Ordinal] = "N"; FieldSize[C.Ordinal] = 1; break; }
                    case "System.DateTime": { FieldType[C.Ordinal] = "D"; FieldSize[C.Ordinal] = 8; break; }
                    case "System.Decimal": { FieldType[C.Ordinal] = "N"; FieldSize[C.Ordinal] = 38; FieldDigs[C.Ordinal] = 5; break; }
                    case "System.Double": { FieldType[C.Ordinal] = "F"; FieldSize[C.Ordinal] = 38; FieldDigs[C.Ordinal] = 5; break; }
                    case "System.Int16": { FieldType[C.Ordinal] = "N"; FieldSize[C.Ordinal] = 6; break; }
                    case "System.Int32": { FieldType[C.Ordinal] = "N"; FieldSize[C.Ordinal] = 11; break; }
                    case "System.Int64": { FieldType[C.Ordinal] = "N"; FieldSize[C.Ordinal] = 21; break; }
                    case "System.SByte": { FieldType[C.Ordinal] = "N"; FieldSize[C.Ordinal] = 6; break; }
                    case "System.Single": { FieldType[C.Ordinal] = "F"; FieldSize[C.Ordinal] = 38; FieldDigs[C.Ordinal] = 5; break; }
                    case "System.UInt16": { FieldType[C.Ordinal] = "N"; FieldSize[C.Ordinal] = 6; break; }
                    case "System.UInt32": { FieldType[C.Ordinal] = "N"; FieldSize[C.Ordinal] = 11; break; }
                    case "System.UInt64": { FieldType[C.Ordinal] = "N"; FieldSize[C.Ordinal] = 21; break; }
                }
                s = s + FieldSize[C.Ordinal];
            }
            buffer = new byte[]{
                (byte)(s % 0x100),
                (byte)(s / 0x100)
            }; // size of header 11-12 bytes
            FS.Write(buffer, 0, buffer.Length);
            for (int j = 0; j < 17; j++) { FS.WriteByte(0x00); } // not used — 17 bytes
            buffer = new byte[] { 0x26, 0x00, 0x00 }; // and more 3 bytes
            FS.Write(buffer, 0, buffer.Length); // Summary: 32 bytes — database descriptor
            
            // fill header
            foreach (DataColumn C in DT.Columns)
            {
                buffer = System.Text.Encoding.Default.GetBytes(FieldName[C.Ordinal]); // field name
                FS.Write(buffer, 0, buffer.Length);
                buffer = new byte[]{
                    System.Text.Encoding.ASCII.GetBytes(FieldType[C.Ordinal])[0],
                    0x00,
                    0x00,
                    0x00,
                    0x00
                }; // size
                FS.Write(buffer, 0, buffer.Length);
                buffer = new byte[]{
                    FieldSize[C.Ordinal],
                    FieldDigs[C.Ordinal]
                }; // dimension
                FS.Write(buffer, 0, buffer.Length);
                buffer = new byte[]{
                    0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00
                }; // 14 zeros
                FS.Write(buffer, 0, buffer.Length);
            }
            FS.WriteByte(0x0D); // end of description

            var data = new DataDbf() { FieldType = FieldType, FieldSize = FieldSize, FS = FS };
            return data;
        }

        private static void DataTableSaveToDBFEnd(FileStream FS)
        {
            FS.WriteByte(0x1A); // end of data
            FS.Close();
        }

        class DataDbf
        {
            public string[] FieldType { get; set; }
            public byte[] FieldSize { get; set; }
            public FileStream FS { get; set; }
        }

        private static void DataRowSaveToDBF(DataRow R, FileStream FS, string[] FieldType, byte[] FieldSize, int encoding)
        {
            NumberFormatInfo nfi = new CultureInfo("en-US", false).NumberFormat;
            string Spaces = "";
            while (Spaces.Length < 255) Spaces = Spaces + " ";

            var DT = R.Table;
            FS.WriteByte(0x20); // write data
            foreach (DataColumn C in DT.Columns)
            {
                string l = R[C].ToString();
                if (l != "") // check for NULL
                {
                    switch (FieldType[C.Ordinal])
                    {
                        case "L":
                            l = bool.Parse(l).ToString();
                            break;
                        case "N":
                            l = decimal.Parse(l).ToString(nfi);
                            break;
                        case "F":
                            l = float.Parse(l).ToString(nfi);
                            break;
                        case "D":
                            l = DateTime.Parse(l).ToString("yyyyMMdd");
                            break;
                        default:
                            l = l.Trim() + Spaces;
                            break;
                    }
                }
                else
                {
                    if (FieldType[C.Ordinal] == "C"
                    || FieldType[C.Ordinal] == "D")
                        l = Spaces;
                }
                while (l.Length < FieldSize[C.Ordinal]) { l = l + (char)0x00; }
                l = l.Substring(0, FieldSize[C.Ordinal]); // correcting size
                var buffer = Encoding.GetEncoding(encoding).GetBytes(l); // write in encoding
                FS.Write(buffer, 0, buffer.Length);
            }
        }
    }
}