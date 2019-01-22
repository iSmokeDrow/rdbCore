using System;
using System.Collections.Generic;
using rdbCore.Structures;
using MoonSharp.Interpreter;

namespace rdbCore
{
    public class LUA
    {
        Script engine = null;
        private string scriptCode = null;

        public LUA(string scriptCode)
        {
            engine = new Script();
            this.scriptCode = scriptCode;
            addGlobals();
            UserData.RegisterType<Row>();
        }

        public string FileName
        {
            get
            {
                var name = engine.Globals["fileName"];
                return (name != null) ? name.ToString() : null;
            }
        }

        public string TableName
        {
            get
            {
                var name = engine.Globals["tableName"];
                return (name != null) ? name.ToString() : null;
            }
        }

        public bool UseRowProcessor { get { return engine.Globals["ProcessRow"] != null; } }

        public bool UseSelectStatement { get { return engine.Globals["selectStatement"] != null; } }

        public string SelectStatement { get { return engine.Globals["selectStatement"].ToString(); } }

        public bool UseSqlColumns { get { return engine.Globals["sqlColumns"] != null; } }

        public List<string> SqlColumns
        {
            get
            {
                List<string> ret = new List<string>();

                try
                {
                    Table t = (Table)engine.Globals["sqlColumns"];
                    for (int tIdx = 1; tIdx < t.Length + 1; tIdx++) { ret.Add(t.Get(tIdx).String); }

                    return ret;
                }
                catch (Exception ex) { throw new Exception(ex.Message, ex.InnerException); }
            }
        }

        public bool ReadHeader
        {
            get
            {
                object ret = engine.Globals["readHeader"];
                return (ret != null) ? (bool)ret : true;
            }
        }

        public bool UseHeader { get { return engine.Globals["header"] != null; } }

        public bool SpecialCase { get { return engine.Globals["specialCase"] != null; } }

        public string Case { get { return engine.Globals["specialCase"].ToString(); } }

        public bool UseExt { get { return engine.Globals["ext"] != null; } }

        public string Ext { get { return engine.Globals["ext"].ToString(); } }

        private void addGlobals()
        {
            #region Type Globals

            engine.Globals["BYTE"] = "byte";
            engine.Globals["BIT_VECTOR"] = "bitvector";
            engine.Globals["BIT_FROM_VECTOR"] = "bitfromvector";
            engine.Globals["INT16"] = "short";
            engine.Globals["SHORT"] = "short";
            engine.Globals["UINT16"] = "ushort";
            engine.Globals["USHORT"] = "ushort";
            engine.Globals["INT32"] = "int";
            engine.Globals["INT"] = "int";
            engine.Globals["UINT32"] = "uint";
            engine.Globals["UINT"] = "uint";
            engine.Globals["INT64"] = "long";
            engine.Globals["LONG"] = "long";
            engine.Globals["SINGLE"] = "single";
            engine.Globals["FLOAT"] = "single";
            engine.Globals["FLOAT32"] = "single";
            engine.Globals["DOUBLE"] = "double";
            engine.Globals["FLOAT64"] = "double";
            engine.Globals["DECIMAL"] = "decimal";
            engine.Globals["DATETIME"] = "datetime";
            engine.Globals["SID"] = "sid";
            engine.Globals["STRING"] = "string";            
            engine.Globals["STRING_LEN"] = "stringlen";
            engine.Globals["STRING_BY_LEN"] = "stringbylen";
            engine.Globals["STRING_BY_REF"] = "stringbyref";

            #endregion

            #region Direction Globals

            engine.Globals["READ"] = "read";
            engine.Globals["WRITE"] = "write";

            #endregion

            #region Special Case Globals

            engine.Globals["DOUBLELOOP"] = "doubleloop";
            engine.Globals["LOOPCOUNTER"] = "loopcounter";
            engine.Globals["ITEMREF"] = "itemref";
            engine.Globals["ROWCOUNT"] = "rowcount";

            #endregion
        }

        public List<LuaField> GetFieldList(string tableName)
        {
            List<LuaField> lFields = new List<LuaField>();

            DynValue res = engine.DoString(scriptCode);

            Table t = (Table)engine.Globals[tableName];

            for (int tIdx = 1; tIdx < t.Length + 1; tIdx++)
            {
                Table fieldT = t.Get(tIdx).Table;
                LuaField lField = new LuaField();

                lField.Name = fieldT.Get(1).String;
                lField.Type = fieldT.Get(2).String;
                lField.BitsName = fieldT.Get("bits_field").String; // TODO: Change me to dependency
                lField.RefName = fieldT.Get("ref_field").String; // TODO: Change me to dependency
                lField.Length = (int)fieldT.Get("length").Number;
                lField.Default = (object)fieldT.Get("default").ToObject();
                lField.Position = (int)fieldT.Get("bit_position").Number;
                lField.Flag = fieldT.Get("flag").String;
                lField.Dependency = fieldT.Get("dependency").String;
                bool showVal = (fieldT.Get("show").ToObject() != null) ? Convert.ToBoolean(fieldT.Get("show").Number) : true;
                if (lField.Type == "stringlen") { showVal = false; }
                lField.Show = showVal;

                lFields.Add(lField);
            }

            return lFields;
        }

        public void CallRowProcessor(string mode, Row row, int rowNum)
        {          
            DynValue res = null;

            try { res = engine.Call(engine.Globals["ProcessRow"], mode, row, rowNum); }
            catch (ScriptRuntimeException srEx) { throw new Exception(srEx.Message, srEx.InnerException);  }        
        }
    }    
}
