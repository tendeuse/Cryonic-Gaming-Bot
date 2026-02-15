using System.Security.Cryptography;
using System.Text;

namespace ArcOverlay.Data;

public static class TokenProtector
{
    private static readonly byte[] Entropy = Encoding.UTF8.GetBytes("ArcOverlay.EVE.SSO");

    public static string Protect(string plaintext)
    {
        var bytes = Encoding.UTF8.GetBytes(plaintext);
        var protectedBytes = ProtectedData.Protect(bytes, Entropy, DataProtectionScope.CurrentUser);
        return Convert.ToBase64String(protectedBytes);
    }

    public static string Unprotect(string encrypted)
    {
        var bytes = Convert.FromBase64String(encrypted);
        var unprotected = ProtectedData.Unprotect(bytes, Entropy, DataProtectionScope.CurrentUser);
        return Encoding.UTF8.GetString(unprotected);
    }
}
