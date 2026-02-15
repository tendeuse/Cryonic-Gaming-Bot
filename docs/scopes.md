# EVE SSO Scopes (MVP)

- `esi-skills.read_skills.v1`: required for `skills_trained` objectives.
- `esi-characters.read_standings.v1`: required for `standings_at_least` objectives.
- `esi-wallet.read_character_wallet.v1`: required for `wallet_isk_change` objectives.
- `esi-wallet.read_corporation_wallets.v1`: optional future corp-level tracking (disabled by default).

Only request scopes enabled in settings to minimize privilege.
