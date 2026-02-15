namespace ArcOverlay.Core.ESI;

public record EsiTokenSet(string AccessToken, string RefreshToken, DateTimeOffset ExpiresAt, string CharacterName);

public record EsiScopes(IReadOnlyList<string> Values)
{
    public string Joined => string.Join(' ', Values);
}
