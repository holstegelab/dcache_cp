# dcache tools

Tools for working with dCache:

- **`dcache_cp`** — Copy files to/from dCache with **Adler-32 checksum verification**.
  Downloads include automatic **tape staging** (bulk pin) and optional destaging.
- **`dcache_ls`** — List dCache directory contents like `ls`, with file locality
  and pin status.

## Requirements

- Python ≥ 3.10
- [`rclone`](https://rclone.org/) configured with a dCache WebDAV remote
- [`ada`](https://github.com/sara-nl/SpiderScripts) (SURF's dCache API tool) for checksums and staging

## Installation

```bash
# From the dcache_cp directory:
pip install .

# Or in development / editable mode:
pip install -e .
```

This installs the `dcache_cp` and `dcache_ls` commands.

## Quick start

```bash
# Upload a directory to dCache
dcache_cp ./experiment_results/ dcache:/results/ -R

# Download from dCache (stages from tape automatically, in batches)
dcache_cp dcache:/results/ ./local_copy/ -R

# Download using a custom remote prefix (uses ~/macaroons/analysis.conf)
dcache_cp analysis:/archive/run42/ ./run42/ -R

# From a two-column TSV file list
dcache_cp --file-list transfers.tsv

# With quota tracking in the progress bar
dcache_cp dcache:/data/ ./data/ -R --quota-pool agh_rwtapepools
```

## How it works

### Direction detection

The `dcache:` (or any custom) prefix on **source** or **destination** determines
the transfer direction:

| Command | Direction |
|---|---|
| `dcache_cp ./local dcache:/remote` | Upload |
| `dcache_cp dcache:/remote ./local` | Download |

### Config file resolution

The prefix name selects the rclone config / macaroon file:

| Prefix | Config file tried first |
|---|---|
| `dcache:` | `~/macaroons/dcache.conf` |
| `analysis:` | `~/macaroons/analysis.conf` |
| `mystore:` | `~/macaroons/mystore.conf` |

If not found in `~/macaroons/`, the tool searches project-level directories:

- **Snellius**: reads the `MYQUOTA_PROJECTSPACES` environment variable
  (space-separated project names) and checks
  `/gpfs/work*/0/<project>/macaroons/<prefix>.conf`.
  Falls back to the user's UNIX group names when the env var is unset.
- **Spider**: uses the user's UNIX group memberships to check
  `/project/<group>/Data/macaroons/<prefix>.conf` — no directory scanning.

Falls back to `$RCLONE_CONFIG`, `~/config/rclone/rclone.conf`,
`~/.config/rclone/rclone.conf` if no match is found.
Use `--config` to override explicitly.

### Pool sidecar file

Place a plain-text `.pool` file next to the config to enable automatic quota
tracking without `--quota-pool`:

```
# ~/macaroons/dcache.pool
agh_rwtapepools
```

When `dcache_cp` resolves `~/macaroons/dcache.conf`, it also checks for
`~/macaroons/dcache.pool`.  If found, the poolgroup inside is used for
`ada --space` quota queries — no need for `--quota-pool` on every invocation.

### Upload flow

1. Enumerate local files
2. For each file (parallel, `--workers` threads):
   - Skip if remote checksum already matches (`--no-skip-verified` to disable)
   - `rclone copyto` local → remote
   - Fetch remote Adler-32 via `ada --checksum`
   - Retry on mismatch (up to `--max-retries`)

### Download flow

1. Enumerate remote files via `rclone lsjson`
2. Process in batches of `--stage-batch` (default 50) to avoid exceeding staging area:
   - **Stage** the batch via `ada --stage --from-file`
   - **Poll** each file; as soon as a file is ONLINE, start downloading it immediately
   - **Download** in parallel with `rclone copyto`, verify Adler-32
   - **Destage** the batch after all files in it are verified
3. Repeat for the next batch

This pipeline approach means the staging area only holds one batch at a time,
so it works even when the total dataset is larger than the available staging
space.

Use `--no-stage` if files are already online.  Use `--no-destage` to keep
them pinned.

### File list mode

Instead of source/destination arguments, provide a two-column TSV file:

```tsv
# Upload example
./sample_01.bam	dcache:/bams/sample_01.bam
./sample_02.bam	dcache:/bams/sample_02.bam

# Download example
dcache:/results/out.vcf	./results/out.vcf
dcache:/results/out.bam	./results/out.bam
```

Rules:
- Tab-separated, two columns: source and destination
- Exactly one column must have a remote prefix (e.g. `dcache:`)
- All rows must be the same direction (all uploads or all downloads)
- Lines starting with `#` are comments; blank lines are skipped

```bash
dcache_cp --file-list transfers.tsv
```

### Quota tracking

Pass `--quota-pool <poolgroup>` to show live dCache storage quota in the
progress bar.  The quota is fetched via `ada --space <poolgroup>` and
refreshed every 2 minutes.

```bash
dcache_cp ./data/ dcache:/archive/data/ -R --quota-pool agh_rwtapepools
```

The progress bar will show available space, pinned capacity, and totals.
The final summary also prints the current quota state.

## Options

```
positional arguments:
  source               Source path (prefix with <remote>: for dCache)
  destination          Destination path (prefix with <remote>: for dCache)

options:
  --file-list TSV      Two-column TSV: source<TAB>destination per line
  -R, --recursive      Copy directories recursively
  --config PATH        rclone config file override
  --remote NAME        rclone remote name (default: only section in config)
  --ada CMD            ada executable (default: ada or $ADA)
  --api URL            dCache API URL override
  --dry-run            Show planned transfers without copying
  --no-skip-verified   Re-transfer even if checksum already matches
  --workers N          Concurrent transfer threads (default: 4)
  --max-retries N      Max retries on checksum mismatch (default: 3)
  --retry-wait SEC     Seconds between retries (default: 60)
  --copy-timeout VAL   rclone --timeout value (default: 300m)
  --no-stage           Skip staging (download only)
  --no-destage         Keep files staged after download
  --stage-batch N      Files to stage per batch (default: 50)
  --stage-timeout SEC  Max wait for staging (default: 86400 = 24h)
  --stage-poll SEC     Poll interval for staging (default: 60)
  --stage-lifetime DUR Pin lifetime (default: 7D)
  --quota-pool GROUP   Show live quota from ada --space in progress bar
  --verbose            Debug logging
  --version            Show version
```

## Environment variables

| Variable | Effect |
|---|---|
| `RCLONE_CONFIG` | Default rclone config path |
| `RCLONE_REMOTE` | Default remote name |
| `ADA` | Path to ada executable |
| `DCACHE_API` / `ADA_API` | dCache API URL |
| `MYQUOTA_PROJECTSPACES` | Space-separated project names for Snellius config discovery |

## Example output

```
2026-04-01 10:00:00 [INFO] config  : /home/user/macaroons/dcache.conf
2026-04-01 10:00:00 [INFO] remote  : dcache_webdav
2026-04-01 10:00:00 [INFO] mode    : upload
2026-04-01 10:00:00 [INFO] files   : 42 (1.3GiB)
2026-04-01 10:00:00 [INFO] workers : 4  retries: 3  skip-verified: yes

  ████████████████████░░░░░░░░░░  28/42 files  890.2MiB/1.3GiB  05:12<01:58  | quota: 2.1TiB avail / 10.0TiB total  pinned: 1.5TiB

2026-04-01 10:05:30 [INFO]
2026-04-01 10:05:30 [INFO] ====== transfer summary ======
2026-04-01 10:05:30 [INFO]   direction : upload
2026-04-01 10:05:30 [INFO]   planned   : 42 files, 1.3GiB
2026-04-01 10:05:30 [INFO]   uploaded  : 42 files, 1.3GiB
2026-04-01 10:05:30 [INFO]   elapsed   : 05:30
2026-04-01 10:05:30 [INFO]   speed     : 4.1MiB/s
2026-04-01 10:05:30 [INFO]   status    : COMPLETED
2026-04-01 10:05:30 [INFO] ==============================
```

---

## dcache_ls

List dCache directory contents like the standard `ls` command, with
optional columns for file locality (ONLINE / NEARLINE), pin lifetime,
and Adler-32 checksums.

### Quick start

```bash
# Simple listing
dcache_ls dcache:/data/

# Long format with human-readable sizes
dcache_ls -lh dcache:/data/

# Show pin lifetime and locality
dcache_ls -l --pin dcache:/data/

# Recursive listing with checksums
dcache_ls -lR --checksum dcache:/data/

# Custom prefix (uses ~/macaroons/analysis.conf)
dcache_ls -l analysis:/archive/run42/
```

### Output

The long format (`-l`) shows permissions, owner, group, size, date,
and file name — just like `ls -l`.  Extra columns are added with flags:

| Flag | Column | Description |
|---|---|---|
| *(default with -l)* | locality | File locality: ONLINE (green), NEARLINE (yellow) |
| `--pin` | pin | Pin/staging lifetime remaining |
| `--checksum` | checksum | Adler-32 checksum (one API call per file) |
| `--no-locality` | | Hide the locality column |

Directories are shown in bold blue, symlinks in cyan.  ONLINE files
are green, NEARLINE files are yellow.  Colors respect `NO_COLOR` and
non-TTY output.

The summary line at the bottom shows file/directory counts and an
online/nearline breakdown.

### Options

```
positional arguments:
  path                 Remote path (prefix with <remote>:)

options:
  -l, --long           Long listing format
  -h, --human-readable Human-readable sizes (e.g. 1.5GiB)
  -R, --recursive      List directories recursively
  --pin                Show pin/staging lifetime column
  --locality           Show file locality column (default with -l)
  --no-locality        Hide file locality column
  --checksum           Show Adler-32 checksum column
  --config PATH        rclone config file override
  --remote NAME        rclone remote name
  --ada CMD            ada executable (default: ada or $ADA)
  --api URL            dCache API URL override
  --version            Show version
```

---

## License

MIT
