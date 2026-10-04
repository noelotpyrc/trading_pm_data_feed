# FotMob team alias audit — 2026-10-04

`fotmob_team_aliases.csv` is the independent PM-name → FotMob team-ID mapping.
It now contains **398 verified PM names**; **467** of the 865 audited names remain unverified.
This update adds **65** aliases without changing any of the previous 333 mappings.

The four live gaps were missing provider-specific name variants: Côte d'Ivoire → Ivory Coast (6709),
CA Mineiro → Atlético-MG (10272), Red Bull Bragantino → RB Bragantino (109705),
and New York Red Bulls → Red Bull New York (6514).

## Verification and scope

- Refetched the 11 configured men's FotMob competition endpoints on 2026-10-04.
- Compared PM names, PM-supplied aliases, and verified SofaScore canonical names against
  participants in the PM-listed competitions. Normalization handles accents and punctuation;
  there is no fuzzy matching or automatic removal of club words.
- Reviewed the remaining abbreviated, translated, and reordered provider names explicitly.
- Confirmed every added ID using `/api/data/teams?id=<id>`: matching ID, team type, male gender,
  and membership in the corresponding competition response. National identities have country `INT`.
- All participant IDs returned by the 11 queried competition responses have at least one
  verified PM alias after this update, including the 195 participants in the 2026 friendly fixtures.
  This is a dated inventory check, not a guarantee of all historical PM names or future fixtures.
- Remaining unverified aliases stay excluded. The audit records
  `verified_exact_pm_alias_and_competition` or `verified_reviewed_name_and_competition` for additions.
- Runtime still requires exact FotMob team IDs, competition, kickoff tolerance, and an
  unambiguous fixture. Provider names are readable provenance, not runtime search queries.

Local source responses and reproducible audit scripts are in the ignored evidence bundle
`data/fotmob-alias-fix-20261004/`: league/team responses, proposed and verified mappings,
`fetch-evidence.py`, `audit.py`, and `verify-proposals.py`. The runtime CSV retains the
competition evidence URL for each alias. Its audit CSV accounts for all 865 PM names.

## Added aliases

| PM name | FotMob name | ID | Verification |
|---|---|---:|---|
| 1. FC Union Berlin | Union Berlin | [8149](https://www.fotmob.com/api/data/teams?id=8149) | Reviewed variant |
| 1. FSV Mainz 05 | Mainz 05 | [9905](https://www.fotmob.com/api/data/teams?id=9905) | Reviewed variant |
| AC Milan | Milan | [8564](https://www.fotmob.com/api/data/teams?id=8564) | Reviewed variant |
| AC Sparta Praha | Sparta Prague | [10247](https://www.fotmob.com/api/data/teams?id=10247) | Reviewed variant |
| AS Monaco FC | Monaco | [9829](https://www.fotmob.com/api/data/teams?id=9829) | Exact PM alias / canonical name |
| AS Roma | Roma | [8686](https://www.fotmob.com/api/data/teams?id=8686) | Exact PM alias / canonical name |
| Ararat-Armenia FA | Ararat Armenia | [866109](https://www.fotmob.com/api/data/teams?id=866109) | Reviewed variant |
| Atlante FC | Atlante | [1942](https://www.fotmob.com/api/data/teams?id=1942) | Reviewed variant |
| Atlas FC | Atlas | [6577](https://www.fotmob.com/api/data/teams?id=6577) | Exact PM alias / canonical name |
| Atlético San Luis | Atlético de San Luis | [6358](https://www.fotmob.com/api/data/teams?id=6358) | Reviewed variant |
| Bayer 04 Leverkusen | Bayer Leverkusen | [8178](https://www.fotmob.com/api/data/teams?id=8178) | Exact PM alias / canonical name |
| Beşiktaş JK | Beşiktaş | [10188](https://www.fotmob.com/api/data/teams?id=10188) | Reviewed variant |
| CA Mineiro | Atlético-MG | [10272](https://www.fotmob.com/api/data/teams?id=10272) | Reviewed variant |
| CA Paranaense | Athletico Paranaense | [10273](https://www.fotmob.com/api/data/teams?id=10273) | Exact PM alias / canonical name |
| CD Guadalajara | Chivas | [7807](https://www.fotmob.com/api/data/teams?id=7807) | Reviewed variant |
| CF Monterrey | Monterrey | [7849](https://www.fotmob.com/api/data/teams?id=7849) | Exact PM alias / canonical name |
| CF Pachuca | Pachuca | [7848](https://www.fotmob.com/api/data/teams?id=7848) | Exact PM alias / canonical name |
| Cabo Verde | Cape Verde | [5888](https://www.fotmob.com/api/data/teams?id=5888) | Reviewed variant |
| Club Brugge KV | Club Brugge | [8342](https://www.fotmob.com/api/data/teams?id=8342) | Exact PM alias / canonical name |
| Club León FC | León | [1841](https://www.fotmob.com/api/data/teams?id=1841) | Exact PM alias / canonical name |
| Club Necaxa | Necaxa | [1842](https://www.fotmob.com/api/data/teams?id=1842) | Exact PM alias / canonical name |
| Club Puebla | Puebla | [7847](https://www.fotmob.com/api/data/teams?id=7847) | Exact PM alias / canonical name |
| Club Tijuana | Tijuana | [162418](https://www.fotmob.com/api/data/teams?id=162418) | Exact PM alias / canonical name |
| Côte d'Ivoire | Ivory Coast | [6709](https://www.fotmob.com/api/data/teams?id=6709) | Reviewed variant |
| Deportivo Toluca FC | Toluca | [6618](https://www.fotmob.com/api/data/teams?id=6618) | Exact PM alias / canonical name |
| FC Augsburg | Augsburg | [8406](https://www.fotmob.com/api/data/teams?id=8406) | Exact PM alias / canonical name |
| FC Barcelona | Barcelona | [8634](https://www.fotmob.com/api/data/teams?id=8634) | Exact PM alias / canonical name |
| FC Bayern München | Bayern München | [9823](https://www.fotmob.com/api/data/teams?id=9823) | Reviewed variant |
| FC Red Bull Salzburg | Salzburg | [10013](https://www.fotmob.com/api/data/teams?id=10013) | Reviewed variant |
| FC Schalke 04 | Schalke 04 | [10189](https://www.fotmob.com/api/data/teams?id=10189) | Reviewed variant |
| FC Viktoria Plzeň | Viktoria Plzeň | [6033](https://www.fotmob.com/api/data/teams?id=6033) | Reviewed variant |
| Ferencvárosi TC | Ferencváros | [8222](https://www.fotmob.com/api/data/teams?id=8222) | Exact PM alias / canonical name |
| GNK Dinamo Zagreb | Dinamo Zagreb | [10156](https://www.fotmob.com/api/data/teams?id=10156) | Reviewed variant |
| Houston Dynamo | Houston Dynamo FC | [8259](https://www.fotmob.com/api/data/teams?id=8259) | Reviewed variant |
| Levante UD | Levante | [8581](https://www.fotmob.com/api/data/teams?id=8581) | Exact PM alias / canonical name |
| Lillestrøm SK | Lillestrøm | [8476](https://www.fotmob.com/api/data/teams?id=8476) | Reviewed variant |
| Liverpool FC | Liverpool | [8650](https://www.fotmob.com/api/data/teams?id=8650) | Exact PM alias / canonical name |
| Macau | Macao | [4175](https://www.fotmob.com/api/data/teams?id=4175) | Reviewed variant |
| Málaga CF | Málaga | [9864](https://www.fotmob.com/api/data/teams?id=9864) | Reviewed variant |
| New York Red Bulls | Red Bull New York | [6514](https://www.fotmob.com/api/data/teams?id=6514) | Reviewed variant |
| Olympiakós SFP | Olympiacos | [8638](https://www.fotmob.com/api/data/teams?id=8638) | Reviewed variant |
| Olympique Lyonnais | Lyon | [9748](https://www.fotmob.com/api/data/teams?id=9748) | Exact PM alias / canonical name |
| Olympique de Marseille | Marseille | [8592](https://www.fotmob.com/api/data/teams?id=8592) | Exact PM alias / canonical name |
| Orlando City SC | Orlando City | [267810](https://www.fotmob.com/api/data/teams?id=267810) | Reviewed variant |
| Pumas de la UNAM | Pumas | [1946](https://www.fotmob.com/api/data/teams?id=1946) | Reviewed variant |
| RC Deportivo A Coruña | Deportivo A Coruña | [9783](https://www.fotmob.com/api/data/teams?id=9783) | Reviewed variant |
| RC Strasbourg Alsace | Strasbourg | [9848](https://www.fotmob.com/api/data/teams?id=9848) | Exact PM alias / canonical name |
| RSC Anderlecht | Anderlecht | [8635](https://www.fotmob.com/api/data/teams?id=8635) | Reviewed variant |
| RU Saint-Gilloise | Union St.Gilloise | [7978](https://www.fotmob.com/api/data/teams?id=7978) | Exact PM alias / canonical name |
| Racing Club de Lens | Lens | [8588](https://www.fotmob.com/api/data/teams?id=8588) | Exact PM alias / canonical name |
| Real Racing Club | Racing Santander | [8696](https://www.fotmob.com/api/data/teams?id=8696) | Reviewed variant |
| Red Bull Bragantino | RB Bragantino | [109705](https://www.fotmob.com/api/data/teams?id=109705) | Reviewed variant |
| SC Freiburg | Freiburg | [8358](https://www.fotmob.com/api/data/teams?id=8358) | Exact PM alias / canonical name |
| SK Puntigamer Sturm Graz | Sturm Graz | [10014](https://www.fotmob.com/api/data/teams?id=10014) | Exact PM alias / canonical name |
| SK Slavia Praha | Slavia Prague | [7787](https://www.fotmob.com/api/data/teams?id=7787) | Reviewed variant |
| SSC Napoli | Napoli | [9875](https://www.fotmob.com/api/data/teams?id=9875) | Exact PM alias / canonical name |
| SV 07 Elversberg | Elversberg | [8232](https://www.fotmob.com/api/data/teams?id=8232) | Reviewed variant |
| SV Werder Bremen | Werder Bremen | [8697](https://www.fotmob.com/api/data/teams?id=8697) | Reviewed variant |
| Stade Brestois 29 | Brest | [8521](https://www.fotmob.com/api/data/teams?id=8521) | Exact PM alias / canonical name |
| Stade Rennais FC 1901 | Rennes | [9851](https://www.fotmob.com/api/data/teams?id=9851) | Exact PM alias / canonical name |
| TSG 1899 Hoffenheim | Hoffenheim | [8226](https://www.fotmob.com/api/data/teams?id=8226) | Exact PM alias / canonical name |
| Tigres de la UANL | Tigres | [8561](https://www.fotmob.com/api/data/teams?id=8561) | Reviewed variant |
| United Arab Emirates | UAE | [5789](https://www.fotmob.com/api/data/teams?id=5789) | Reviewed variant |
| Viking FK | Viking | [8478](https://www.fotmob.com/api/data/teams?id=8478) | Reviewed variant |
| ŠK Slovan Bratislava | Slovan Bratislava | [6019](https://www.fotmob.com/api/data/teams?id=6019) | Reviewed variant |
