# ChangeLogs

## Version: 1.0 
### Date: 24/07/2024
### Maintainer: Freezing
Finally, the first version created with many issues (I think so :D), include:
- Schemas: Define new data model from old data collected by Redfish API (HPEiLO/iDRAC)
- Configs: Define metrics collection for prometheus exporter

## Version: 2.x.x
### Date: 12/08/2025
### Maintainer: Freezing
Maybe we can use this version for a long time:
- Drop user/pass params when curl, it's too ridiculous :D
- we can extend more workers for improving performance

## Version: 2.3.0
### Date: 02/11/2026
### Maintainer: Freezing
- Debug artifacts: opt-in normalized snapshot via `loglevel=debug` + `Tuning.Debug.WriteNormalizedData`, written to a hashed per-target/config directory under `Tuning.Debug.RootDirectory` (see README "Debug a scrape")
- Debug artifacts: a failed write now logs `errno` and `stage` (`root_directory_path` / `root_directory` / `target_directory`) to pinpoint the unwritable directory
- Deps: bump aiohttp 3.14.3 -> 3.14.4
- CI: all jobs run on the in-cluster `falcon` runner; fork PRs are skipped; PR images are built on a `/build` comment and pushed as `pr-<PR_NUMBER>`