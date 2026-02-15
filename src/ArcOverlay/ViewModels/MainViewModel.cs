using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using ArcOverlay.Services;

namespace ArcOverlay.ViewModels;

public sealed class MainViewModel : INotifyPropertyChanged
{
    private readonly MissionFeedService _missionFeedService = new();
    private string _status = "Initializing…";

    public ObservableCollection<MissionCardViewModel> Missions { get; } = new();

    public string Status
    {
        get => _status;
        set
        {
            if (_status == value) return;
            _status = value;
            OnPropertyChanged();
        }
    }

    public event PropertyChangedEventHandler? PropertyChanged;

    public async Task StartAsync()
    {
        var initial = await _missionFeedService.GetInitialMissionsAsync();
        Missions.Clear();
        foreach (var mission in initial)
        {
            Missions.Add(mission);
        }

        Status = "Connected. Polling mission updates every 30s.";
        _ = Task.Run(PollLoopAsync);
    }

    private async Task PollLoopAsync()
    {
        while (true)
        {
            await Task.Delay(TimeSpan.FromSeconds(30));
            try
            {
                var updates = await _missionFeedService.GetUpdatedMissionsAsync();
                await Application.Current.Dispatcher.InvokeAsync(() =>
                {
                    foreach (var update in updates)
                    {
                        var existing = Missions.FirstOrDefault(m => m.MissionId == update.MissionId);
                        if (existing is null)
                        {
                            Missions.Add(update);
                        }
                        else
                        {
                            existing.Title = update.Title;
                            existing.Lore = update.Lore;
                            existing.ProgressText = update.ProgressText;
                        }
                    }
                    Status = $"Last sync: {DateTime.Now:T}";
                });
            }
            catch (Exception ex)
            {
                Status = $"Sync error: {ex.Message}";
            }
        }
    }

    private void OnPropertyChanged([CallerMemberName] string? propertyName = null)
    {
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
    }
}

public sealed class MissionCardViewModel : INotifyPropertyChanged
{
    private string _title = string.Empty;
    private string _lore = string.Empty;
    private string _progressText = "Pending";

    public string MissionId { get; init; } = string.Empty;

    public string Title { get => _title; set { _title = value; OnPropertyChanged(); } }
    public string Lore { get => _lore; set { _lore = value; OnPropertyChanged(); } }
    public string ProgressText { get => _progressText; set { _progressText = value; OnPropertyChanged(); } }

    public event PropertyChangedEventHandler? PropertyChanged;

    private void OnPropertyChanged([CallerMemberName] string? propertyName = null)
    {
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
    }
}
