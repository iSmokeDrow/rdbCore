using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace rdbCore.Structures
{
    public class Cell
    {
        internal LuaField Info;

        public string Name { get { return Info.Name; } }
        public string Type { get { return Info.Type; } }
        public int Length { get { return Info.Length; } set { Info.Length = value; } }
        public object Value { get; set; }
        public object Default { get { return Info.Default; } }

    }
}
