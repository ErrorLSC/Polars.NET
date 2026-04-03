using System.Runtime.InteropServices;

namespace Polars.NET.Core.Native;

[StructLayout(LayoutKind.Sequential)]
public struct PlMissingColumnsPolicyOrExprC
{
    public PlMissingColumnsPolicyType PolicyType;
    public IntPtr ExprPtr; 
}

[StructLayout(LayoutKind.Sequential)]
public struct PlMatchToSchemaPerColumnC
{
    public PlMissingColumnsPolicyOrExprC MissingColumns;
    public PlMissingColumnsPolicy MissingStructFields;
    public PlExtraColumnsPolicy ExtraStructFields;
    public PlUpcastOrForbid IntegerCast;
    public PlUpcastOrForbid FloatCast;
}

[StructLayout(LayoutKind.Sequential)]
public struct PlSchemaColumnOverrideC
{
    public IntPtr ColName;
    public PlMatchToSchemaPerColumnC Config;
}
