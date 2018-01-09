namespace rdbCore.Structures
{
    public enum CellType
    {
        Byte = 0,
        BitVector = 1,
        BitFromVector = 2,
        Short = 3,
        UShort = 4,
        Int = 5,
        UInt = 6,
        Single = 7,
        Double = 8,
        Decimal = 9,
        SID = 10,
        String = 11,
        StringLen = 12,
        StringByLen = 13,
        StringByRef = 14,
        Loop = 15
    }

    public enum HeaderType
    {
        Traditional = 0,
        Defined = 1
    }
}
