using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.IO;
using System.Threading.Tasks;
using rdbCore.Functions;
using rdbCore.Structures;

namespace rdbCore
{
    public class Core
    {
        #region Properties

        public Encoding Encoding = Encoding.Default;
        protected string structurePath;
        protected List<LuaField> fieldList; 
        protected string rdbPath;
        protected string date = string.Empty;
        protected Row header;
        protected List<Row> data = new List<Row>();
        protected LUA luaIO;

        private List<Cell> rowCells;


        #endregion

        #region Constructors

        public Core() { }

        public Core(Encoding encoding) { this.Encoding = encoding; }

        #endregion

        #region Events

        public event EventHandler<ProgressMaxArgs> ProgressMaxChanged;
        public event EventHandler<ProgressValueArgs> ProgressValueChanged;
        public event EventHandler<MessageArgs> MessageOccured;

        #endregion

        #region Event Delegates

        public void OnProgressMaxChanged(ProgressMaxArgs p) { ProgressMaxChanged?.Invoke(this, p); }
        public void OnProgressValueChanged(ProgressValueArgs p) { ProgressValueChanged?.Invoke(this, p); }
        public void OnMessageOccured(MessageArgs m) { MessageOccured?.Invoke(this, m); }

        #endregion

        #region Public Propeerties

        public string FileName { get { return luaIO.FileName; } }

        public string TableName { get { return luaIO.TableName; } }

        public string CreatedDate { get { return date; } }

        public string Case { get { return luaIO.Case; } }

        public string Extension { get { return (luaIO.UseExt) ? luaIO.Ext : "rdb"; } }

        public List<Row> Data { get { return data; } }

        public int FieldCount { get { return fieldList.Count; } }

        public List<LuaField> HeaderList { get { return (UseHeader) ? luaIO.GetFieldList("header") : null; } }

        public List<LuaField> FieldList { get { return fieldList; } }

        public bool UseRowProcesser { get { return luaIO.UseRowProcessor; } }

        public bool SpecialCase { get { return luaIO.SpecialCase; } }

        public bool UseSelectStatement { get { return luaIO.UseSelectStatement; } }

        public string SelectStatement { get { return luaIO.SelectStatement; } }

        public bool UseSqlColumns {  get { return luaIO.UseSqlColumns; } }
       
        public string SqlColumns
        {
            get
            {
                string ret = string.Empty;
                List<string> columns = luaIO.SqlColumns;

                for (int i = 0; i < columns.Count; i++) { ret += string.Format("{0},\n", columns[i]); }
                ret = ret.Remove(ret.Length - 3, 3);

                return ret;
            }
        }

        public SqlCommand InsertStatement
        {
            get
            {
                SqlCommand sqlCmd = new SqlCommand();
                string columns = string.Empty;
                string parameters = string.Empty;

                if (UseSqlColumns)
                {
                    List<string> sqlColumns = luaIO.SqlColumns;
                    int colCount = sqlColumns.Count;

                    OnProgressMaxChanged(new ProgressMaxArgs(colCount));

                    for (int colIdx = 0; colIdx < colCount; colIdx++)
                    {
                        string val = sqlColumns[colIdx];
                        string columnType = GetField(val).Type;
                        columns += string.Format("{0}{1},", val, string.Empty);
                        parameters += string.Format("@{0}{1},", val, string.Empty);
                        SqlDbType paramType = SqlDbType.Int;

                        switch (columnType)
                        {
                            case "short":
                                paramType = SqlDbType.SmallInt;
                                break;

                            case "ushort":
                                paramType = SqlDbType.SmallInt;
                                break;

                            case "int":
                                paramType = SqlDbType.Int;
                                break;

                            case "uint":
                                paramType = SqlDbType.Int;
                                break;

                            case "long":
                                paramType = SqlDbType.BigInt;
                                break;

                            case "byte":
                                paramType = SqlDbType.TinyInt;
                                break;

                            case "datetime":
                                paramType = SqlDbType.DateTime;
                                break;

                            case "decimal":
                                paramType = SqlDbType.Decimal;
                                break;

                            case "single":
                                paramType = SqlDbType.Float;
                                break;

                            case "double":
                                paramType = SqlDbType.Float;
                                break;

                            case "string":
                                paramType = SqlDbType.VarChar;
                                break;

                            case "stringbyref":
                                paramType = SqlDbType.VarChar;
                                break;
                        }
                        sqlCmd.Parameters.Add(val, paramType);

                        if (((colIdx * 100) / colCount) != ((colIdx - 1) * 100 / colCount)) { OnProgressValueChanged(new ProgressValueArgs(colIdx)); }
                    }               
                }
                else
                {
                    int colCount = fieldList.Count;

                    OnProgressMaxChanged(new ProgressMaxArgs(colCount));

                    for (int colIdx = 0; colIdx < colCount; colIdx++)
                    {
                        LuaField field = fieldList[colIdx];

                        if (field.Show)
                        {
                            columns += string.Format("{0}{1},", field.Name, string.Empty);
                            parameters += string.Format("@{0}{1},", field.Name, string.Empty);
                            SqlDbType paramType = SqlDbType.Int;

                            switch (field.Type)
                            {
                                case "short":
                                    paramType = SqlDbType.SmallInt;
                                    break;

                                case "ushort":
                                    paramType = SqlDbType.SmallInt;
                                    break;

                                case "int":
                                    paramType = SqlDbType.Int;
                                    break;

                                case "uint":
                                    paramType = SqlDbType.Int;
                                    break;

                                case "long":
                                    paramType = SqlDbType.BigInt;
                                    break;

                                case "byte":
                                    paramType = SqlDbType.TinyInt;
                                    break;

                                case "datetime":
                                    paramType = SqlDbType.DateTime;
                                    break;

                                case "decimal":
                                    paramType = SqlDbType.Decimal;
                                    break;

                                case "single":
                                    paramType = SqlDbType.Float;
                                    break;

                                case "double":
                                    paramType = SqlDbType.Float;
                                    break;

                                case "string":
                                    paramType = SqlDbType.NVarChar;
                                    break;

                                case "stringbyref":
                                    paramType = SqlDbType.VarChar;
                                    break;
                            }
                            sqlCmd.Parameters.Add(field.Name, paramType);

                        }

                        if (((colIdx * 100) / colCount) != ((colIdx - 1) * 100 / colCount)) { OnProgressValueChanged(new ProgressValueArgs(colIdx)); }
                    }
                }

                OnProgressValueChanged(new ProgressValueArgs(0));
                OnProgressMaxChanged(new ProgressMaxArgs(100));

                sqlCmd.CommandText = string.Format("INSERT INTO <tableName> ({0}) VALUES ({1})", columns.Remove(columns.Length - 1, 1), parameters.Remove(parameters.Length - 1, 1));
                return sqlCmd;
            }
        }

        public int RowCount { get { return data.Count; } }

        public bool ReadHeader { get { return (luaIO != null) ? luaIO.ReadHeader : false; } }

        public bool UseHeader {  get { return (luaIO != null) ? luaIO.UseHeader : false; } }

        #endregion

        #region Methods (Public)

        public void Initialize(string structurePath)
        {
            luaIO = new LUA(IO.LoadStructure(structurePath));
            fieldList = luaIO.GetFieldList("fields");
            if (luaIO.UseHeader) { header = new Row(luaIO.GetFieldList("header")); }
        }

        public Row GetRow(int idx) { return (Row)data[idx]; }

        public LuaField GetField(int idx) { return fieldList[idx]; }

        public LuaField GetField(string name) { return fieldList.Find(f => f.Name == name); }

        public int GetFieldIdx(string name) { return fieldList.FindIndex(f => f.Name == name); }

        public void CallRowProcessor(string mode, Row row, int rowCount) { luaIO.CallRowProcessor(mode, row, rowCount); }

        public void SetEncoding(Encoding encoding) { this.Encoding = encoding; }

        public void SetData(List<Row> data) { this.data = data; }

        public void ClearData() { data.Clear(); }

        void parseBuffer(byte[] fileBytes)
        {
            using (MemoryStream ms = new MemoryStream(fileBytes, false))
            {
                int rowCnt = 0;

                byte[] buffer = new byte[0];

                if (luaIO.UseHeader)
                {
                    header = readHeader(ms);
                    rowCnt = (int)header.ValueByFlag("rowcount");
                    // TODO: Define 'date' by reading actual info (if present)
                    date = string.Format("{0}{1}{2}", DateTime.Now.Year, GetDate(DateTime.Now.Month), GetDate(DateTime.Now.Day));
                }
                else
                {
                    buffer = new byte[8];
                    ms.Read(buffer, 0, buffer.Length);

                    date = this.Encoding.GetString(buffer);

                    ms.Position += 120;

                    // Read the row count
                    buffer = new byte[4];
                    ms.Read(buffer, 0, buffer.Length);
                    rowCnt = BitConverter.ToInt32(buffer, 0);
                }

                if (SpecialCase)
                {
                    OnProgressMaxChanged(new ProgressMaxArgs(rowCnt));

                    switch (Case)
                    {
                        case "doubleloop":

                            for (int rowIdx = 0; rowIdx < rowCnt; rowIdx++)
                            {
                                buffer = new byte[4];
                                ms.Read(buffer, 0, buffer.Length);

                                int loopCount = BitConverter.ToInt32(buffer, 0);
                                for (int i = 0; i < loopCount; i++)
                                {
                                    Row currentRow = readRow(ms);
                                    if (UseRowProcesser) { CallRowProcessor("read", currentRow, rowIdx); }
                                    data.Add(currentRow);
                                    if (((rowIdx * 100) / RowCount) != ((rowIdx - 1) * 100 / RowCount)) { OnProgressValueChanged(new ProgressValueArgs(rowIdx)); }
                                }
                            }

                            break;
                    }
                }
                else
                {
                    OnProgressMaxChanged(new ProgressMaxArgs(rowCnt));

                    for (int rowIdx = 0; rowIdx < rowCnt; rowIdx++)
                    {
                        Row currentRow = readRow(ms);
                        if (UseRowProcesser) { CallRowProcessor("read", currentRow, rowIdx); }
                        data.Add(currentRow);

                        if (((rowIdx * 100) / rowCnt) != ((rowIdx - 1) * 100 / rowCnt)) { OnProgressValueChanged(new ProgressValueArgs(rowIdx)); }
                    }
                }
            }

            OnProgressMaxChanged(new ProgressMaxArgs(100));
            OnProgressValueChanged(new ProgressValueArgs(0));
        }

        public void ParseRDB(string rdbPath)
        {
            if (File.Exists(rdbPath))
            {
                parseBuffer(File.ReadAllBytes(rdbPath));
            }
            else { throw new FileNotFoundException("Cannot find file specified", rdbPath); }
        }

        public void ParseRDB(byte[] fileBytes) { parseBuffer(fileBytes); }

        public void WriteRDB(string buildPath)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                writeHeader(ms, (luaIO.UseHeader) ? HeaderType.Defined : HeaderType.Traditional);
              
                OnProgressMaxChanged(new ProgressMaxArgs(RowCount));

                if (SpecialCase)
                {
                    byte[] buffer;

                    switch (Case)
                    {
                        case "doubleloop":

                            int previousVal = 0;

                            for (int rowIdx = 0; rowIdx < RowCount; rowIdx++)
                            {
                                Row currentRow = (Row)data[rowIdx];
                                int currentVal = (int)currentRow.ValueByFlag("loopcounter");
                                if (previousVal != currentVal)
                                {
                                    List<Row> rows = data.FindAll(r => (int)r[0] == currentVal);

                                    buffer = BitConverter.GetBytes(rows.Count);
                                    ms.Write(buffer, 0, buffer.Length);

                                    for (int filteredIdx = 0; filteredIdx < rows.Count; filteredIdx++)
                                    {
                                        if (UseRowProcesser) { CallRowProcessor("write", rows[filteredIdx], rowIdx); }
                                        writeRow(ms, rows[filteredIdx]);
                                    }

                                    previousVal = currentVal;
                                }
                            }

                            break;
                    }
                }
                else
                {                  
                    for (int rowIdx = 0; rowIdx < RowCount; rowIdx++)
                    {
                        Row currentRow = data[rowIdx];

                        if (UseRowProcesser) { CallRowProcessor("write", currentRow, rowIdx); }
                        writeRow(ms, currentRow);

                        if (((rowIdx * 100) / RowCount) != ((rowIdx - 1) * 100 / RowCount)) { OnProgressValueChanged(new ProgressValueArgs(rowIdx)); }
                    }
                }

                OnMessageOccured(new MessageArgs(string.Format("Writing {0}", buildPath)));
                using (FileStream fs = File.Create(buildPath)) { ms.WriteTo(fs); }
            }

            OnProgressMaxChanged(new ProgressMaxArgs(100));
            OnProgressValueChanged(new ProgressValueArgs(0));
        }

        #endregion

        #region Methods (Private)

        private BitVector32 generateBitVector(Row currentRow, string fieldName)
        {
            List<Cell> cells = currentRow.GetBitFromVectorFields(fieldName);
            BitVector32 bitVector = currentRow.GetBitVector(fieldName);

            foreach (Cell cell in cells) { bitVector[1 << cell.Info.Position] = Convert.ToBoolean(cell.Value); }
            return bitVector;
        }

        private byte[] readStream(MemoryStream ms, int size)
        {
            byte[] buffer = new byte[size];
            ms.Read(buffer, 0, buffer.Length);
            return buffer;
        }

        private Row readHeader(MemoryStream ms)
        {
            Row header = new Row(luaIO.GetFieldList("header"));
            for (int cellIdx = 0; cellIdx < header.Count; cellIdx++)
            {
                Cell cell = header.GetCell(cellIdx);

                switch (cell.Type)
                {
                    case "short":
                        header[cellIdx] = BitConverter.ToInt16(readStream(ms, cell.Length), 0);
                        break;

                    case "ushort":
                        header[cellIdx] = BitConverter.ToUInt16(readStream(ms, cell.Length), 0);
                        break;

                    case "int":
                        header[cellIdx] = BitConverter.ToInt32(readStream(ms, cell.Length), 0);
                        break;

                    case "uint":
                        header[cellIdx] = BitConverter.ToUInt32(readStream(ms, cell.Length), 0);
                        break;

                    case "byte":
                        header[cellIdx] = (int)readStream(ms, cell.Length)[0];
                        break;

                    case "date": // TODO: Implement me
                        break;
                }
            }

            return header;
        }

        private Row readRow(MemoryStream ms)
        {
            Row row = new Row(fieldList);

            for (int fieldIdx = 0; fieldIdx < row.Count; fieldIdx++)
            {
                LuaField currentField = GetField(fieldIdx);

                switch (currentField.Type)
                {
                    case "short":
                        row[fieldIdx] = BitConverter.ToInt16(readStream(ms, currentField.Length), 0);
                        break;

                    case "ushort":
                        row[fieldIdx] = BitConverter.ToUInt16(readStream(ms, currentField.Length), 0);
                        break;

                    case "int":
                        row[fieldIdx] = BitConverter.ToInt32(readStream(ms, currentField.Length), 0);
                        break;

                    case "uint":
                        row[fieldIdx] = BitConverter.ToUInt32(readStream(ms, currentField.Length), 0);
                        break;

                    case "long":
                        row[fieldIdx] = BitConverter.ToUInt64(readStream(ms, currentField.Length), 0);
                        break;

                    case "byte":
                        row[fieldIdx] = (int)readStream(ms, currentField.Length)[0];
                        break;

                    case "bitvector":
                        row[fieldIdx] = new BitVector32(BitConverter.ToInt32(readStream(ms, currentField.Length), 0));
                        break;

                    case "bitfromvector":
                        int bitPos = row.GetPosition(fieldIdx);
                        BitVector32 bitVector = row.GetBitVector(currentField.BitsName);
                        row[fieldIdx] = Convert.ToInt32(bitVector[1 << bitPos]);
                        break;

                    case "datetime":
                        int val = BitConverter.ToInt32(readStream(ms, currentField.Length), 0);
                        row[fieldIdx] = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddSeconds(val);
                        break;

                    case "decimal":
                        int v0 = BitConverter.ToInt32(readStream(ms, currentField.Length), 0);
                        decimal v1 = v0 / 100m;
                        row[fieldIdx] = v1;
                        break;

                    case "single":
                        row[fieldIdx] = BitConverter.ToSingle(readStream(ms, currentField.Length), 0);
                        break;

                    case "double":
                        row[fieldIdx] = BitConverter.ToDouble(readStream(ms, currentField.Length), 0);
                        break;

                    case "sid":
                        row[fieldIdx] = RowCount;
                        break;

                    case "string":
                        row[fieldIdx] = ByteConverterExt.ToString(readStream(ms, currentField.Length), Encoding);
                        break;

                    case "stringlen":
                        row[fieldIdx] = BitConverter.ToInt32(readStream(ms, currentField.Length), 0);
                        break;

                    case "stringbylen":
                        {
                            int len = 0;
                            int.TryParse(row[currentField.Dependency].ToString(), out len);

                            if (len < 0)
                                break;
                            else
                                row[fieldIdx] = ByteConverterExt.ToString(readStream(ms, len), Encoding);
                        }
                        break;

                    case "stringbyref":
                        int refLen = (int)header.GetRefValue(currentField.RefName);
                        row[fieldIdx] = ByteConverterExt.ToString(readStream(ms, refLen), Encoding);
                        break;
                }
            }

            return row;
        }

        private void writeHeader(MemoryStream ms, HeaderType type)
        {
            switch (type)
            {
                case HeaderType.Traditional:

                    byte[] tmpHeader = new byte[128];
                    byte[] date = this.Encoding.GetBytes(string.Format("{0}{1}{2}", DateTime.Now.Year, GetDate(DateTime.Now.Month), GetDate(DateTime.Now.Day)));
                    for (int i = 0; i < date.Length; i++) { tmpHeader[i] = date[i]; }
                    ms.Write(tmpHeader, 0, tmpHeader.Length);
                    if (SpecialCase)
                    {
                        switch (Case)
                        {
                            case "doubleloop":
                                byte[] buffer;
                                int previousVal = 0;
                                int loopCount = 0;

                                for (int rowIdx = 0; rowIdx < RowCount; rowIdx++)
                                {
                                    int currentVal = (int)((Row)data[rowIdx]).ValueByFlag("loopcounter");
                                    if (previousVal != currentVal) { previousVal = currentVal; loopCount++; }
                                }

                                buffer = BitConverter.GetBytes(loopCount);
                                ms.Write(buffer, 0, buffer.Length);
                                break;
                        }
                    }
                    else { ms.Write(BitConverter.GetBytes(RowCount), 0, 4); }

                    break;

                case HeaderType.Defined:

                    // Update header ROWCOUNT field incase user added new entries
                    header[header.IdxByFlag("rowcount")] = RowCount;

                    for (int cellIdx = 0; cellIdx < header.Count; cellIdx++)
                    {
                        Cell cell = header.GetCell(cellIdx);
                        if (cell.Value == null) { cell.Value = cell.Default; }

                        switch (cell.Type)
                        {
                            case "short":
                                ms.Write(BitConverter.GetBytes(Convert.ToInt16(cell.Value)), 0, cell.Length);
                                break;

                            case "ushort":
                                ms.Write(BitConverter.GetBytes(Convert.ToUInt16(cell.Value)), 0, cell.Length);
                                break;

                            case "int":
                                ms.Write(BitConverter.GetBytes(Convert.ToInt32(cell.Value)), 0, cell.Length);
                                break;

                            case "uint":
                                ms.Write(BitConverter.GetBytes(Convert.ToUInt32(cell.Value)), 0, cell.Length);
                                break;

                            case "byte":
                                int fieldLen = cell.Length;
                                if (fieldLen == 1) { ms.WriteByte(Convert.ToByte(cell.Value)); }
                                else { ms.Write(new byte[fieldLen], 0, fieldLen); }
                                break;

                            case "date": // TODO: Implement me
                                break;
                        }
                    }

                    break;
            }
        }

        private void writeRow(MemoryStream ms, Row currentRow)
        {
            byte[] buffer; 

            for (int fieldIdx = 0; fieldIdx < FieldCount; fieldIdx++)
            {
                LuaField currentField = GetField(fieldIdx);
                if (currentRow[fieldIdx] == null) { currentRow[fieldIdx] = currentRow.GetCell(fieldIdx).Default; }

                switch (currentField.Type)
                {
                    case "short":
                        ms.Write(BitConverter.GetBytes(Convert.ToInt16(currentRow[fieldIdx])), 0, currentField.Length);
                        break;

                    case "ushort":
                        ms.Write(BitConverter.GetBytes(Convert.ToUInt16(currentRow[fieldIdx])), 0, currentField.Length);
                        break;

                    case "int":
                        ms.Write(BitConverter.GetBytes(Convert.ToInt32(currentRow[fieldIdx])), 0, currentField.Length);
                        break;

                    case "uint":
                        ms.Write(BitConverter.GetBytes(Convert.ToUInt32(currentRow[fieldIdx])), 0, currentField.Length);
                        break;

                    case "long":
                        ms.Write(BitConverter.GetBytes(Convert.ToInt64(currentRow[fieldIdx])), 0, currentField.Length);
                        break;

                    case "byte":
                        int fieldLen = currentRow.GetLength(fieldIdx);
                        if (fieldLen == 1) { ms.WriteByte(Convert.ToByte(currentRow[fieldIdx])); }
                        else { ms.Write(new byte[fieldLen], 0, fieldLen); }              
                        break;

                    case "bitvector":
                        ms.Write(BitConverter.GetBytes(generateBitVector(currentRow, currentField.Name).Data), 0, currentField.Length);           
                        break;

                    case "datetime":
                        DateTime val = Convert.ToDateTime(currentRow[currentField.Name]);
                        int val2 = Convert.ToInt32((val - new DateTime(1970, 1, 1, 0, 0, 0, 0)).TotalSeconds);
                        ms.Write(BitConverter.GetBytes(val2), 0, currentField.Length);
                        break;

                    case "decimal":
                        decimal v0 = Convert.ToDecimal(currentRow[fieldIdx]);
                        int v1 = Convert.ToInt32(v0 * 100);
                        ms.Write(BitConverter.GetBytes(v1), 0, currentField.Length);
                        break;

                    case "single":
                        ms.Write(BitConverter.GetBytes(Convert.ToSingle(currentRow[fieldIdx])), 0, currentField.Length);
                        break;

                    case "double":
                        ms.Write(BitConverter.GetBytes(Convert.ToDouble(currentRow[fieldIdx])), 0, currentField.Length);
                        break;

                    case "string":
                        {
                            buffer = ByteConverterExt.ToBytes(currentRow[fieldIdx].ToString(), Encoding);
                            int remainder = currentField.Length - buffer.Length;
                            ms.Write(buffer, 0, buffer.Length);
                            ms.Write(new byte[remainder], 0, remainder);
                        }
                        break;

                    case "stringlen":
                        ms.Write(BitConverter.GetBytes(currentRow.GetStringByLenValue(currentField.Name).Length + 1), 0, currentField.Length);
                        break;

                    case "stringbylen":
                        buffer = ByteConverterExt.ToBytes(currentRow[fieldIdx].ToString() + '\0', Encoding);
                        ms.Write(buffer, 0, buffer.Length);
                        break;

                    case "stringbyref": // TODO: Implement me [stringbyref] SAVE
                        {
                            buffer = ByteConverterExt.ToBytes(currentRow[fieldIdx].ToString(), Encoding);
                            string refName = currentRow.GetRefName(fieldIdx).ToString();
                            int remainder = Convert.ToInt32(header[refName]) - buffer.Length;
                            ms.Write(buffer, 0, buffer.Length);
                            ms.Write(new byte[remainder], 0, remainder);
                        }
                        break;
                }
            }
        }

        private string GetDate(int pInt)
        {
            if (pInt >= 10)
                return pInt.ToString();
            else
                return string.Format("0{0}", pInt);
        }

        #endregion
    }
}
