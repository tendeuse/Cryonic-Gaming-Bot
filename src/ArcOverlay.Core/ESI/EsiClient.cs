using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using ArcOverlay.Core.Models;

namespace ArcOverlay.Core.ESI;

public sealed class EsiClient
{
    private const string AuthorizeUrl = "https://login.eveonline.com/v2/oauth/authorize";
    private const string TokenUrl = "https://login.eveonline.com/v2/oauth/token";
    private readonly HttpClient _httpClient;

    public EsiClient(HttpClient httpClient)
    {
        _httpClient = httpClient;
        _httpClient.BaseAddress = new Uri("https://esi.evetech.net/latest/");
    }

    public string BuildAuthorizationUrl(string clientId, string redirectUri, EsiScopes scopes, string codeChallenge, string state)
    {
        var encodedRedirect = Uri.EscapeDataString(redirectUri);
        var encodedScopes = Uri.EscapeDataString(scopes.Joined);
        return $"{AuthorizeUrl}?response_type=code&redirect_uri={encodedRedirect}&client_id={clientId}&scope={encodedScopes}&state={state}&code_challenge={codeChallenge}&code_challenge_method=S256";
    }

    public async Task<EsiTokenSet> ExchangeCodeAsync(string clientId, string code, string codeVerifier, string redirectUri, CancellationToken cancellationToken)
    {
        var payload = new Dictionary<string, string>
        {
            ["grant_type"] = "authorization_code",
            ["code"] = code,
            ["client_id"] = clientId,
            ["code_verifier"] = codeVerifier,
            ["redirect_uri"] = redirectUri
        };

        using var content = new FormUrlEncodedContent(payload);
        using var response = await _httpClient.PostAsync(TokenUrl, content, cancellationToken);
        response.EnsureSuccessStatusCode();

        var json = await response.Content.ReadAsStringAsync(cancellationToken);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        return new EsiTokenSet(
            root.GetProperty("access_token").GetString()!,
            root.GetProperty("refresh_token").GetString()!,
            DateTimeOffset.UtcNow.AddSeconds(root.GetProperty("expires_in").GetInt32()),
            "Unknown Capsuleer");
    }

    public async Task<CharacterSnapshot> SnapshotAsync(string accessToken, CancellationToken cancellationToken)
    {
        _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);

        var standings = await GetStandingsAsync(cancellationToken);
        var skills = await GetSkillsAsync(cancellationToken);
        var wallet = await GetWalletAsync(cancellationToken);

        return new CharacterSnapshot(standings, new Dictionary<long, long>(), skills, wallet);
    }

    private async Task<IReadOnlyDictionary<long, double>> GetStandingsAsync(CancellationToken cancellationToken)
    {
        using var resp = await _httpClient.GetAsync("characters/{character_id}/standings/", cancellationToken);
        if (resp.StatusCode is HttpStatusCode.TooManyRequests or (HttpStatusCode)420)
        {
            return new Dictionary<long, double>();
        }

        if (!resp.IsSuccessStatusCode)
        {
            return new Dictionary<long, double>();
        }

        var body = await resp.Content.ReadAsStringAsync(cancellationToken);
        using var doc = JsonDocument.Parse(body);
        var result = new Dictionary<long, double>();
        foreach (var item in doc.RootElement.EnumerateArray())
        {
            if (item.GetProperty("from_type").GetString() != "npc_corp")
            {
                continue;
            }

            result[item.GetProperty("from_id").GetInt64()] = item.GetProperty("standing").GetDouble();
        }

        return result;
    }

    private async Task<IReadOnlyDictionary<int, int>> GetSkillsAsync(CancellationToken cancellationToken)
    {
        using var resp = await _httpClient.GetAsync("characters/{character_id}/skills/", cancellationToken);
        if (!resp.IsSuccessStatusCode)
        {
            return new Dictionary<int, int>();
        }

        var body = await resp.Content.ReadAsStringAsync(cancellationToken);
        using var doc = JsonDocument.Parse(body);
        var result = new Dictionary<int, int>();
        foreach (var item in doc.RootElement.GetProperty("skills").EnumerateArray())
        {
            result[item.GetProperty("skill_id").GetInt32()] = item.GetProperty("trained_skill_level").GetInt32();
        }

        return result;
    }

    private async Task<decimal> GetWalletAsync(CancellationToken cancellationToken)
    {
        using var resp = await _httpClient.GetAsync("characters/{character_id}/wallet/", cancellationToken);
        if (!resp.IsSuccessStatusCode)
        {
            return 0;
        }

        var body = await resp.Content.ReadAsStringAsync(cancellationToken);
        return decimal.TryParse(body, out var isk) ? isk : 0;
    }
}
