using System.Net.Http.Json;
using System.Text.Json;
using ArcOverlay.ViewModels;

namespace ArcOverlay.Services;

public sealed class MissionFeedService
{
    private readonly HttpClient _httpClient;
    private DateTimeOffset _lastUpdate = DateTimeOffset.UnixEpoch;

    public MissionFeedService()
    {
        var apiBase = Environment.GetEnvironmentVariable("API_BASE_URL") ?? "http://127.0.0.1:8010";
        _httpClient = new HttpClient { BaseAddress = new Uri(apiBase) };
    }

    public async Task<IReadOnlyList<MissionCardViewModel>> GetInitialMissionsAsync()
    {
        var packs = await _httpClient.GetFromJsonAsync<JsonElement>("/api/v1/packs/default-caldari");
        if (packs.ValueKind == JsonValueKind.Undefined)
        {
            return Array.Empty<MissionCardViewModel>();
        }

        var missions = packs.GetProperty("missions").EnumerateArray();
        var list = new List<MissionCardViewModel>();
        foreach (var mission in missions)
        {
            list.Add(new MissionCardViewModel
            {
                MissionId = mission.GetProperty("mission_id").GetString() ?? "",
                Title = mission.GetProperty("title").GetString() ?? "Untitled",
                Lore = mission.GetProperty("lore").GetString() ?? "",
                ProgressText = "Awaiting ESI sync"
            });
        }

        _lastUpdate = DateTimeOffset.UtcNow;
        return list;
    }

    public async Task<IReadOnlyList<MissionCardViewModel>> GetUpdatedMissionsAsync()
    {
        var updates = await _httpClient.GetFromJsonAsync<JsonElement>($"/api/v1/updates?since={Uri.EscapeDataString(_lastUpdate.ToString("O"))}");
        _lastUpdate = DateTimeOffset.UtcNow;

        if (!updates.TryGetProperty("changed_missions", out var changed))
        {
            return Array.Empty<MissionCardViewModel>();
        }

        return changed.EnumerateArray().Select(m => new MissionCardViewModel
        {
            MissionId = m.GetProperty("mission_id").GetString() ?? "",
            Title = m.GetProperty("title").GetString() ?? "Untitled",
            Lore = m.GetProperty("lore").GetString() ?? "",
            ProgressText = "Updated from feed"
        }).ToList();
    }
}
