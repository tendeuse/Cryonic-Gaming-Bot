using System.Runtime.InteropServices;

namespace ArcOverlay;

internal static class Win32Interop
{
    private const int GwlExStyle = -20;
    private const int WsExTransparent = 0x20;
    private const int WsExLayered = 0x80000;

    [DllImport("user32.dll")]
    private static extern int GetWindowLong(IntPtr hWnd, int nIndex);

    [DllImport("user32.dll")]
    private static extern int SetWindowLong(IntPtr hWnd, int nIndex, int dwNewLong);

    public static void SetClickThrough(IntPtr handle, bool enabled)
    {
        var exStyle = GetWindowLong(handle, GwlExStyle);
        if (enabled)
        {
            SetWindowLong(handle, GwlExStyle, exStyle | WsExTransparent | WsExLayered);
        }
        else
        {
            SetWindowLong(handle, GwlExStyle, (exStyle | WsExLayered) & ~WsExTransparent);
        }
    }
}
