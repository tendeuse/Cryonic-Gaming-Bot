namespace ArcOverlay.Core.Models;

public enum AlphaOmegaMode
{
    Alpha,
    Omega,
    Both
}

public record ObjectiveTarget(Dictionary<string, object> Data);

public record MissionObjective(
    string Id,
    string Type,
    ObjectiveTarget Target,
    string Display,
    bool ManualConfirmationAllowed = false);

public record MissionReward(int Ap, string Badge);

public record MissionDefinition(
    string MissionId,
    int Revision,
    string PackId,
    string Title,
    string Lore,
    string Faction,
    IReadOnlyList<MissionObjective> Objectives,
    MissionReward Rewards,
    AlphaOmegaMode AlphaOmega,
    IReadOnlyList<string> Tags,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt,
    bool Deprecated = false);

public record MissionPack(
    string PackId,
    int Revision,
    string Name,
    string Description,
    string Faction,
    bool Published,
    IReadOnlyList<string> MissionIds,
    DateTimeOffset UpdatedAt);

public record MissionUpdateFeed(
    DateTimeOffset ServerTime,
    IReadOnlyList<MissionDefinition> ChangedMissions,
    IReadOnlyList<MissionPack> ChangedPacks,
    IReadOnlyList<string> DeprecatedMissionIds);

public record ObjectiveProgress(string ObjectiveId, double CurrentValue, double TargetValue, bool Completed, string StatusText);

public record MissionProgress(
    string MissionId,
    bool Completed,
    IReadOnlyList<ObjectiveProgress> ObjectiveProgress,
    DateTimeOffset LastEvaluatedAt);

public record CharacterSnapshot(
    IReadOnlyDictionary<long, double> StandingsByNpcCorp,
    IReadOnlyDictionary<long, long> LoyaltyPointsByCorp,
    IReadOnlyDictionary<int, int> SkillLevels,
    decimal WalletIsk);
