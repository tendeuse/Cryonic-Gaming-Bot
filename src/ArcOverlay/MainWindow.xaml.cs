using System.Runtime.InteropServices;
using System.Windows.Input;
using ArcOverlay.ViewModels;

namespace ArcOverlay;

public partial class MainWindow : Window
{
    private readonly MainViewModel _viewModel;
    private bool _clickThroughEnabled;

    [DllImport("user32.dll")]
    private static extern bool RegisterHotKey(IntPtr hWnd, int id, uint fsModifiers, uint vk);

    [DllImport("user32.dll")]
    private static extern bool UnregisterHotKey(IntPtr hWnd, int id);

    private const int WmHotkey = 0x0312;

    public MainWindow()
    {
        InitializeComponent();
        _viewModel = new MainViewModel();
        DataContext = _viewModel;
        Loaded += OnLoaded;
        Closed += OnClosed;
    }

    private void OnLoaded(object sender, RoutedEventArgs e)
    {
        var helper = new System.Windows.Interop.WindowInteropHelper(this);
        var source = System.Windows.Interop.HwndSource.FromHwnd(helper.Handle);
        source?.AddHook(HwndHook);

        RegisterHotKey(helper.Handle, 1, 0, (uint)KeyInterop.VirtualKeyFromKey(Key.F10));
        RegisterHotKey(helper.Handle, 2, 0, (uint)KeyInterop.VirtualKeyFromKey(Key.F9));

        _ = _viewModel.StartAsync();
    }

    private void OnClosed(object? sender, EventArgs e)
    {
        var helper = new System.Windows.Interop.WindowInteropHelper(this);
        UnregisterHotKey(helper.Handle, 1);
        UnregisterHotKey(helper.Handle, 2);
    }

    private IntPtr HwndHook(IntPtr hwnd, int msg, IntPtr wParam, IntPtr lParam, ref bool handled)
    {
        if (msg != WmHotkey)
        {
            return IntPtr.Zero;
        }

        var id = wParam.ToInt32();
        if (id == 1)
        {
            Visibility = Visibility == Visibility.Visible ? Visibility.Hidden : Visibility.Visible;
            handled = true;
        }
        else if (id == 2)
        {
            _clickThroughEnabled = !_clickThroughEnabled;
            Win32Interop.SetClickThrough(hwnd, _clickThroughEnabled);
            _viewModel.Status = _clickThroughEnabled ? "Click-through enabled" : "Click-through disabled";
            handled = true;
        }

        return IntPtr.Zero;
    }
}
