using ArcOverlay.Core.Models;

namespace ArcOverlay.Core.Engine;

public sealed class MissionEngine
{
    private readonly Dictionary<string, Func<MissionObjective, CharacterSnapshot, ObjectiveProgress>> _evaluators;

    public MissionEngine()
    {
        _evaluators = new()
        {
            ["standings_at_least"] = EvaluateStandingsAtLeast,
            ["lp_earned_total"] = EvaluateLpEarned,
            ["skills_trained"] = EvaluateSkillsTrained,
            ["wallet_isk_change"] = EvaluateWalletIskChange
        };
    }

    public MissionProgress Evaluate(MissionDefinition mission, CharacterSnapshot snapshot)
    {
        var objectiveProgress = mission.Objectives
            .Select(objective => _evaluators.TryGetValue(objective.Type, out var evaluator)
                ? evaluator(objective, snapshot)
                : new ObjectiveProgress(objective.Id, 0, 1, false, "Unsupported objective type."))
            .ToList();

        return new MissionProgress(
            mission.MissionId,
            objectiveProgress.All(o => o.Completed),
            objectiveProgress,
            DateTimeOffset.UtcNow);
    }

    private static ObjectiveProgress EvaluateStandingsAtLeast(MissionObjective objective, CharacterSnapshot snapshot)
    {
        var corpId = Convert.ToInt64(objective.Target.Data["npc_corp_id"]);
        var required = Convert.ToDouble(objective.Target.Data["value"]);
        snapshot.StandingsByNpcCorp.TryGetValue(corpId, out var standing);

        return new ObjectiveProgress(
            objective.Id,
            standing,
            required,
            standing >= required,
            $"Standing {standing:F2} / {required:F2}");
    }

    private static ObjectiveProgress EvaluateLpEarned(MissionObjective objective, CharacterSnapshot snapshot)
    {
        var corpId = Convert.ToInt64(objective.Target.Data["npc_corp_id"]);
        var required = Convert.ToDouble(objective.Target.Data["value"]);
        snapshot.LoyaltyPointsByCorp.TryGetValue(corpId, out var earned);

        return new ObjectiveProgress(
            objective.Id,
            earned,
            required,
            earned >= required,
            $"LP {earned:N0} / {required:N0}");
    }

    private static ObjectiveProgress EvaluateSkillsTrained(MissionObjective objective, CharacterSnapshot snapshot)
    {
        var skillId = Convert.ToInt32(objective.Target.Data["skill_id"]);
        var required = Convert.ToDouble(objective.Target.Data["level"]);
        snapshot.SkillLevels.TryGetValue(skillId, out var level);

        return new ObjectiveProgress(
            objective.Id,
            level,
            required,
            level >= required,
            $"Skill level {level} / {required}");
    }

    private static ObjectiveProgress EvaluateWalletIskChange(MissionObjective objective, CharacterSnapshot snapshot)
    {
        var required = Convert.ToDouble(objective.Target.Data["value"]);
        var current = Convert.ToDouble(snapshot.WalletIsk);

        return new ObjectiveProgress(
            objective.Id,
            current,
            required,
            current >= required,
            $"Wallet delta {current:N2} / {required:N2}");
    }
}
