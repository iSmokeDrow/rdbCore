using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace rdbCore.Structures
{
    public class LuaField
    {
        public string Name { get; set; }
        public string Type { get; set; }
        public string BitsName { get; set; }
        public string RefName { get; set; }
        private int _length = 0;
        public int Length
        {
            get { return _length; }
            set
            {
                if (value == 0)
                {
                    switch (Type)
                    {
                        case "byte":
                            _length = 1;
                            break;

                        case "bitvector":
                            _length = 4;
                            break;

                        case "datetime":
                            _length = 4;
                            break;

                        case "decimal":
                            _length = 4;
                            break;

                        case "int":
                            _length = 4;
                            break;

                        case "uint":
                            _length = 4;
                            break;

                        case "long":
                            _length = 8;
                            break;

                        case "short":
                            _length = 2;
                            break;

                        case "single":
                            _length = 4;
                            break;

                        case "double":
                            _length = 8;
                            break;

                        case "stringlen":
                            _length = 4;
                            break;
                    }
                }
                else { _length = value; }              
            }
        }
        private object _default;
        public object Default
        {
            get
            {
                if (Type == "int" | Type == "short" | Type == "byte") { return (_default != null) ? _default : 0; }
                if (Type == "string") { return (_default != null) ? _default : "0"; }

                return null;
            }
            set { _default = value; }
        }
        public int Position { get; set; }
        public string Flag { get; set; }
        public string Dependency { get; set; }
        public bool Show { get; set; } = true;
    }
}
