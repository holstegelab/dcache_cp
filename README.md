# dcache tools

Tools for working with dCache:

- **`dcache_cp`** — Copy files to/from dCache with **Adler-32 checksum verification**.
  Downloads include automatic **tape staging** (bulk pin) and optional destaging.
- **`dcache_mv`** — Move files to/from dCache by running a verified copy and then
  deleting the source file only after success.
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

This installs the `dcache_cp`, `dcache_mv`, and `dcache_ls` commands.

## Quick start

```bash
# Upload a directory to dCache
dcache_cp ./experiment_results/ dcache:/results/ -R

# Move a file into dCache (delete local source after verified upload)
dcache_mv ./sample.bam dcache:/results/sample.bam

# Download from dCache (stages from tape automatically, in batches)
dcache_cp dcache:/results/ ./local_copy/ -R

# Download a single file to an exact local filename
dcache_cp dcache:/results/sample.bam ./renamed.bam

# Move a file out of dCache (delete remote source after verified download)
dcache_mv dcache:/results/sample.bam ./sample.bam

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

### Download destinations and dry runs

A single remote file is copied to the exact local destination filename. If the
destination is an existing directory or ends with `/`, the source filename is
appended instead. Multiple remote sources always use a destination directory.
Directory downloads place the directory's contents under the local destination.

Both `dcache_cp --dry-run` and `dcache_mv --dry-run` stop after planning. They may
list remote paths to build the plan, but do not compare checksums, stage data,
copy files, or delete sources. Already verified files remain in the preview;
checksum-based skipping happens only during an actual transfer.

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
   - Copy to a unique temporary remote file using `rclone copyto --ignore-times`
   - Verify Adler-32 and confirm the local source is unchanged
   - Promote the verified temporary copy with `rclone moveto --ignore-times --no-check-dest`
   - Retry failed copies or verification (up to `--max-retries`)

Existing destinations survive copy and verification failures. Cleanup removes
only the temporary copy. If upload promotion fails, any remaining verified
temporary file is retained and its path is reported for recovery.
Allow space for a complete temporary copy alongside an existing destination.
Interrupted transfers may leave temporary files; inspect them before removing
them or retrying, and retain the source until verification succeeds.

### Download flow

1. Enumerate remote files via `rclone lsjson`
2. Unless `--no-skip-verified` is set, compare existing local files against remote
  checksums first and skip already verified files before staging them
3. Process in batches of `--stage-batch` (default 10000, also limited by `--stage-batch-bytes`) to avoid exceeding staging area:
   - **Stage** the batch via `ada --stage --from-file`
   - If the initial ADA stage command or a per-file stage request fails, `dcache_cp`
     falls back to authenticated 1-byte WebDAV reads to trigger dCache's normal
     on-read staging path for those files
   - **Poll** each file and its PIN request; download once it is ONLINE and its
     requested pin has completed
   - **Download** in parallel to unique local temporary files, verify Adler-32,
     then atomically replace the final filenames
   - **Destage** the batch after its downloads finish, releasing only pins belonging
     to its stage request IDs. Wait for UNPIN completion before staging another batch
4. Repeat for the next batch

At most one batch of this command's explicit pins is active at a time. Released
copies may remain cached, and other jobs share the pool: batch limits are not a
space reservation. WebDAV fallback reads do not guarantee explicit pins.

Use `--no-stage` if files are already online.  Use `--no-destage` to keep
them pinned. With `--no-destage`, the entire retained set must fit the configured
file and byte limits. A file larger than `--stage-batch-bytes` is rejected before
staging; increase the limit explicitly. Failed pin release prevents the next batch.
Cleanup has a 120-second budget. Pending owned PIN requests are cancelled and
settled before their pins are released. Cleanup never releases another request's
pins, including when staging used only the WebDAV fallback.

Each unique remote source is staged once per batch. Copy requests for that source
to several local destinations are all preserved. Bearer tokens are passed to curl
through owner-only temporary header files rather than command-line arguments.
The tool follows ADA's returned bulk-request URLs for status and release, so
this also works with ADA versions lacking `--stat-request`. Paginated target
statuses are checked, and UUIDs in filenames are not mistaken for request IDs.

### Move flow

`dcache_mv` reuses the same transfer engine as `dcache_cp`, but deletes the
source only after the copy has been checksum-verified.

- Upload move: verified upload, then delete the local source file
- Download move: verified download, then delete the remote source file
- Moves use fresh local checksums and recheck source identity/content immediately
  before deletion. Keep move inputs quiescent: external writers must not modify
  the source or destination while a move is running.
- A file symlink upload copies its target and removes only the requested link.
  Moving through a directory symlink is rejected; select its explicit target.
- Recursive moves delete transferred source files, but do not remove now-empty
  source directories
- In `--file-list` mode, resumed move rows are skipped when the source is
  already gone but the destination file already exists

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
- Quoted TSV fields are supported; exactly two columns are required
- Every destination must be unique, with no overlapping file/child paths
- Use one remote prefix per invocation. A move source may appear only once;
  copy it to all required destinations before moving it

`--remote` selects the same config section for rclone and ADA. Both literal
`bearer_token` and `bearer_token_command` configurations are supported; ADA gets
only the selected token in an owner-only temporary config. Percent-encoded URLs
are read literally. `--ada` and help/version output do not trigger default ADA
resolution or downloads. Metadata commands have a 120-second bound, and checksum
and staging calls also respect their remaining command deadlines. Cancellation
stops queued work and terminates active subprocess groups.

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
  path [path ...]      Source(s) and destination. Last argument is the destination
                       (prefix with <remote>: for dCache). Multiple sources supported.

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
  --copy-timeout VAL   rclone idle --timeout value (default: 300m)
  --checksum-timeout N Max seconds to wait for dCache checksum (default: 14400 = 4h)
  --no-stage           Skip staging (download only)
  --no-destage         Keep files staged after download
  --stage-batch N      Files to stage per batch (default: 10000)
  --stage-batch-bytes N Max staged bytes (default: 5497558138880 = 5 TiB)
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
dcache_ls -lH dcache:/data/

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
