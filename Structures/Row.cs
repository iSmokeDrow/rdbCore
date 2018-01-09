using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;

namespace rdbCore.Structures
{
    public class Row : IEnumerable
    {
        internal List<Cell> Cells = new List<Cell>();
        public Row(List<LuaField> fields) { for (int i = 0; i < fields.Count; i++) { Cells.Add(new Cell() { Info = fields[i] }); } }

        public object this[string key]
        {
            get { return Cells.Find(f => f.Info.Name == key).Value; }
            set { Cells.Find(f => f.Info.Name == key).Value = value; }
        }

        public Cell this[CellType key] { get { return Cells.Find(f => f.Type == Enum.GetName(typeof(CellType), key).ToLower()); } }

        public object this[int idx]
        {
            get { return Cells[idx].Value; }
            set { Cells[idx].Value = value; }
        }

        public List<Cell> GetBitFromVectorFields(string fieldName) { return Cells.FindAll(f => f.Info.Type == "bitfromvector" && f.Info.BitsName == fieldName); }

        public int Count { get { return Cells.Count; } }

        public int IdxByFlag(string flag) { return Cells.FindIndex(f => f.Info.Flag == flag); }

        public string KeyByFlag(string flag) { return Cells.Find(f => f.Info.Flag == flag).Info.Name; }

        public object ValueByFlag(string flag) { return Cells.Find(f => f.Info.Flag == flag).Value; }

        public Cell GetCell(int idx) { return Cells[idx]; }

        public bool KeyIsDuplicate(string key) { return Cells.FindAll(f => f.Info.Name == key).Count > 1; }

        public int GetLength(string key) { return Cells.Find(f => f.Info.Name == key).Info.Length; }

        public int GetLength(int idx) { return Cells[idx].Info.Length; }

        public int GetPosition(string key) { return Cells.Find(f => f.Info.Name == key).Info.Position; }

        public int GetPosition(int idx) { return Cells[idx].Info.Position; }

        public BitVector32 GetBitVector(string key) { return (BitVector32)Cells.Find(f => f.Info.Name == key).Value; }

        public int GetStringLen(string key) { return (int)Cells.Find(f => f.Info.Name == key && f.Info.Type == "stringlen").Value; }

        public string GetStringByLenValue(string key) { return Cells.Find(f => f.Info.Name == key && f.Info.Type == "stringbylen").Value.ToString(); }

        /// <summary>
        /// In the case of a field that has a duplicate such db_item (item_use_flag) search for and return the 
        /// value of the shown duplicate field
        /// </summary>
        /// <param name="key">Name of the duplicated field</param>
        /// <returns>The value of the duplicated field that is shown</returns>
        public int GetShownValue(string key) { return (int)Cells.Find(f => f.Info.Name == key).Value; }

        public object GetRefName(string key) { return Cells.Find(f => f.Info.Name == key).Info.RefName; }
        
        public object GetRefName(int idx) { return Cells[idx].Info.RefName; }

        public object GetRefValue(string ref_field) { return Cells.Find(f => f.Name == ref_field).Value; }

        public bool ContainsDuplicate(string key) { return Cells.FindAll(f => f.Info.Name == key).Count > 1; }

        #region IEnumerable Interface

        IEnumerator IEnumerable.GetEnumerator() { return GetEnumerator(); }

        public CellsEnum GetEnumerator() { return new CellsEnum(Cells); }

        #endregion
    }

    public class Cell
    {
        internal LuaField Info;
        
        public string Name { get { return Info.Name; } }
        public string Type { get { return Info.Type; } }
        public int Length { get { return Info.Length; } set { Info.Length = value; } }
        public object Value { get; set; }
        public object Default { get { return Info.Default; } }
    }

    public class CellsEnum : IEnumerator
    {
        List<Cell> cells;

        int position = -1;

        public CellsEnum(List<Cell> cells) { this.cells = cells; }

        public bool MoveNext()
        {
            position++;
            return position < cells.Count;
        }

        public void Reset() { position = -1; }

        object IEnumerator.Current { get { return Current; } }

        public Cell Current { get { return cells[position]; } }        
    }
}