using ArcOverlay.Core.Models;
using Microsoft.Data.Sqlite;

namespace ArcOverlay.Data;

public sealed class OverlayDb
{
    private readonly string _connectionString;

    public OverlayDb(string dbPath)
    {
        _connectionString = new SqliteConnectionStringBuilder { DataSource = dbPath }.ToString();
    }

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await using var conn = new SqliteConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        var commandText = """
            CREATE TABLE IF NOT EXISTS user_profile (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                character_id INTEGER,
                character_name TEXT,
                alpha_omega TEXT,
                faction_focus TEXT,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS tokens (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                access_token TEXT NOT NULL,
                refresh_token TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS missions (
                mission_id TEXT PRIMARY KEY,
                revision INTEGER NOT NULL,
                pack_id TEXT NOT NULL,
                json TEXT NOT NULL,
                deprecated INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS mission_packs (
                pack_id TEXT PRIMARY KEY,
                revision INTEGER NOT NULL,
                json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS mission_progress (
                mission_id TEXT PRIMARY KEY,
                completed INTEGER NOT NULL,
                progress_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_name TEXT NOT NULL,
                details TEXT,
                created_at TEXT NOT NULL
            );
            """;

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = commandText;
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task UpsertMissionAsync(MissionDefinition mission, string json, CancellationToken cancellationToken)
    {
        await using var conn = new SqliteConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            INSERT INTO missions (mission_id, revision, pack_id, json, deprecated, updated_at)
            VALUES ($id, $revision, $pack, $json, $deprecated, $updated)
            ON CONFLICT(mission_id) DO UPDATE SET
                revision = excluded.revision,
                pack_id = excluded.pack_id,
                json = excluded.json,
                deprecated = excluded.deprecated,
                updated_at = excluded.updated_at;";
        cmd.Parameters.AddWithValue("$id", mission.MissionId);
        cmd.Parameters.AddWithValue("$revision", mission.Revision);
        cmd.Parameters.AddWithValue("$pack", mission.PackId);
        cmd.Parameters.AddWithValue("$json", json);
        cmd.Parameters.AddWithValue("$deprecated", mission.Deprecated ? 1 : 0);
        cmd.Parameters.AddWithValue("$updated", DateTimeOffset.UtcNow.ToString("O"));
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }
}
