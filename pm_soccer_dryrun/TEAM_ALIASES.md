# Team alias verification — 2026-09-19 EDT

`soccer_team_aliases.csv` is the bundled runtime
`pm_name,query,sofascore_team_id` whitelist.
It contains **865 unique PM names mapped to verified Sofascore identities**, expanded
from 10 entries. All ten previous PM names remain covered. The `query` column
retains Sofascore's canonical names for readability; runtime matching uses the IDs.

`soccer_team_aliases_audit.csv` records **all 890 distinct PM names** from **1,205 PM
team entries** across the existing 11 league tags, including 25 entries withheld
from the aliases file. Each row includes PM IDs and aliases, league tags,
Sofascore ID/name, verification status, membership evidence, and a review note.
Names shared between league catalogs have one alias and one audit row.

## Results

- **856** aliases have matching team identity and participation evidence in at
  least one of that name's PM-listed competitions, in a checked current or past season.
- **9** additional aliases are confirmed men's national football teams on Sofascore,
  but were absent from the six checked international-friendly seasons. Their status
  is `verified_national_identity`, not verified competition membership.
- **25** entries need review. Some exist on Sofascore but have questionable PM league
  classification; others are ambiguous or have no conclusive match. A missing search
  result does **not** establish that the team does not exist.

| PM league tag | PM teams code | Names | Aliases | Need review | Sofascore tournament IDs |
|---|---|---:|---:|---:|---|
| epl | epl | 25 | 25 | 0 | 17 |
| la-liga | lal | 26 | 26 | 0 | 8 |
| sea | sea | 24 | 24 | 0 | 23 |
| bundesliga | bun | 24 | 24 | 0 | 35 |
| ligue-1 | fl1 | 21 | 21 | 0 | 34 |
| ucl | ucl | 262 | 259 | 3 | 7 |
| uel | uel | 510 | 490 | 20 | 679 |
| mex | mex | 24 | 22 | 2 | 11621, 11620 |
| brazil-serie-a | bra | 35 | 35 | 0 | 325 |
| mls | mls | 32 | 30 | 2 | 242 |
| fifa-friendly | fif | 222 | 221 | 1 | 851 |

League totals overlap because the same club can appear under a domestic league,
UCL and UEL. PM's team catalogs contain historical entries, not just this season's
participants; these counts must not be interpreted as current league sizes.
`matched_leagues` in the audit lists observed membership, and `current_leagues`
means present in the latest primary tournament season returned by Sofascore at
verification time. An empty value does not prove nonmembership. For Mexico, the
primary tournament is Apertura; Clausura membership is also checked historically.

## Key corrections

| PM name | Sofascore query | Sofascore team ID |
|---|---|---:|
| CA Mineiro | Atlético Mineiro | 1977 |
| Associação Chapecoense de Futebol | Chapecoense | 21845 |
| Mirassol FC | Mirassol | 21982 |
| Clube do Remo | Remo | 2012 |
| CA Paranaense | Athletico | 1967 |
| Dinamo 1948 | FC Dinamo București | 3292 |

PM supplies `Atlético Mineiro` as an alias of `CA Mineiro`; the verified Brazilian
competition participant is team 1977. The unrelated `CA Mineiro Humi` search hit
is not used. PM team/provider IDs and Sofascore IDs are separate namespaces.

## Verification method and sources

1. Read the league tags in `sources.TAGS`. Obtain PM sports metadata and paginate
   `https://gamma-api.polymarket.com/teams?league=<code>&limit=100&offset=<offset>`.
   Team league codes differ from discovery tags; for example Brazil uses `bra`.
   Preserve PM names and API-provided aliases rather than inventing PM spellings.
2. Read Sofascore's `/unique-tournament/<id>/seasons` and each checked season's
   `/teams` endpoint. For the latest two seasons also read standings and the first
   previous/upcoming event pages. Competition team endpoints provide the main
   participant lists; event pages are supplemental, not exhaustive fixture scans.
3. Check six most recent seasons per competition initially, expanding UCL, UEL,
   Brazil and Mexico to 18. Mexico also checks 18 Clausura seasons. A team found
   only historically remains distinguishable from a current participant.
4. Match original names and PM-provided aliases against competition participants.
   Review spelling, abbreviation, sponsor and translated-name differences, and
   use Sofascore search for unresolved names. Record the resulting identity ID.
   Exclude women/youth/reserves and non-football teams. Every included identity
   has explicit men's football metadata; national entries also have `national=true`.
5. Keep ambiguous names and questionable league classifications out of the alias
   file. Do not use fuzzy similarity or result popularity alone to approve them.
   For example, `RC Gent` is not assigned to `KAA Gent`, `FC Eindhoven` is not
   assigned to PSV, and `KF Trepça '89` is distinguished from KF Trepça Mitrovicë.

Sofascore API paths are relative to `https://api.sofascore.com/api/v1`.
The audit CSV links one supporting endpoint per row (or the attempted search for
unresolved rows). Supporting records must be checked for the listed **team ID**;
a search response can contain other teams as well.

The local evidence bundle is `data/soccer-team-alias-audit-20260919/`:

- `pm-sports.json`, `pm-teams-*.json`: fetched PM catalogs.
- `sofa-cache/*.json`: timestamped responses, request paths, parameters and HTTP status.
- `competition-teams.json`: IDs, seasons and full membership source paths.
- `final-verification.json`: complete row-level evidence and review decisions.
- `reviewed-mappings.txt`: manually reviewed name-to-ID choices.
- `verify_teams.py`, `extra_checks.py`, `finalize.py`: audit/build scripts.
- `original-aliases.csv`: previous configuration; `summary.json`: counts.

The evidence bundle is local audit material under git-ignored `data/`; the runtime
CSV, compact audit CSV and this report are the durable project files. This audit
is a dated snapshot and must be refreshed as PM adds names and competitions change.

## Runtime scope and validation

`team_whitelist_from()` now loads and validates the 865 verified name-to-ID entries.
The resolver fetches fixtures directly by ID and requires both exact team IDs,
allowing reversed home/away orientation. The existing three-hour kickoff tolerance
and ambiguity check remain. `query` retains the canonical name for readability;
there is **no global team search or popularity-based fallback**.

If either PM name is absent, the match stays unmapped with
`mapping_status=team_not_whitelisted` and its `unverified_teams` recorded. This
includes all 25 flagged names below and any newly introduced PM name until reviewed.
Market recording continues without mapped scores. The whitelist does not recheck
current-season membership live; discovery still uses the existing 11 league tags.

A 404 from one fixture-list direction does not prevent checking the other.
Missing/ambiguous fixtures and transient errors retry after 60 seconds. Per-match
errors, including detail endpoint failures, are isolated so later matches proceed.
Shared 403/429 backoff and fatal recording errors retain their existing behavior.
Successful mappings persist for the session. Diagnostics retain verified IDs in
PM home/away order, unavailable fixture endpoints, and per-match error phase/status.

`--aliases PATH.csv` now requires `sofascore_team_id`; old query-only files fail
startup rather than enabling a search fallback. Blank names, missing/nonpositive
IDs and duplicate PM names are rejected. Each session snapshots the whitelist,
and its content hash is bound into the run manifest. Config changes require a new
data directory. The snapshot is configuration provenance, not a recovery journal.

Validation: **112 tests pass**, including identity enforcement on both sides,
flagged names, reversed fixtures, kickoff/ambiguity checks, 404 fallback, per-match
failure isolation, timed retries, shared backoff, fatal recording errors, and
configuration validation/snapshots. The 865 entries agree with the audit IDs.
Offline replay of saved API responses resolves Mineiro to event **15237986**, also
when `/next` is forced to return 404. All names in the previous run's 76 catalog
matches are covered, and all 14 fixture pairs with recorded Sofascore observations
agree with the whitelist. Reproduction: `data/soccer-resolver-validation-20260919/`.
No new live recorder was started.

## Entries requiring review

| PM name | Reason |
|---|---|
| Víkingur | Multiple senior clubs: Reykjavík, Gøta and Ólafsvík; requires country or fixture evidence. |
| KF Víkingur | Short name does not distinguish the Víkingur clubs; requires fixture evidence. |
| FCI Tallinn | Search returns FC Infonet Tallinn and FCI Levadia Tallinn; historical identity requires review. |
| FK Vardar Brvenica | No exact search result; do not substitute FK Vardar Skopje. |
| MK Maccabi Be'er Sheva | Maccabi Beer Sheva exists (36273); do not substitute Hapoel Beer Sheva. No checked UEL membership. |
| B | One-letter PM name is insufficient; Sofascore search returns HTTP 400. |
| FK Kruoja Pakruojis | Search only found a second-team record; do not substitute Pakruojis without identity evidence. |
| FC Bălți | Identity found, but no membership in the checked seasons of its PM-listed leagues; review scope. |
| Hang Sai SC | Exact Sofascore team exists (320447), but no UEL membership evidence; PM league classification needs review. |
| CDF Benfica | No result for full PM name; do not substitute SL Benfica without identity evidence. |
| Syunik FA | FC Syunik exists (423250); do not substitute Pyunik. No checked UEL membership. |
| Akademisk BK | Search returns several Akademisk/AB records; requires country or fixture evidence. |
| Djurgårdsbrunns FC | No search result; do not substitute Djurgårdens IF. |
| CS Alliance Dudelange | No search result; do not substitute F91 Dudelange. |
| FC Eindhoven | Exact Sofascore team exists (2994), distinct from PSV Eindhoven; no UEL membership evidence in checked seasons. |
| ROC Charleroi | Only a youth result from exact search; do not substitute RC Sporting Charleroi. |
| OFK Grbalj Radanovići | Identity found, but no membership in the checked seasons of its PM-listed leagues; review scope. |
| Yellow-Red KV Mechelen | Identity found, but no membership in the checked seasons of its PM-listed leagues; review scope. |
| RC Gent | Search finds R RC Gent-Zeehaven (115145), not KAA Gent (2903); no membership evidence in checked UEL seasons. |
| F91 Diddeleng U19 | Explicit youth team; do not alias to senior F91 Dudelange. |
| CA Morelia | Historical Morelia/Mazatlán record and current Atlético Morelia are distinct IDs; requires fixture evidence. |
| Chiapas FC | Search returns Jaguares and other Chiapas clubs; historical identity requires review. |
| MLS All-Stars | Sofascore team exists (24000), but is an all-star selection, not a regular MLS club. |
| Liga MX All-Stars | Sofascore team exists (337601), but is an all-star selection, not a regular MLS club. |
| Mandatory Palestine | Historical national-team label; do not infer current Palestine identity. |
