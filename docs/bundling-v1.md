# Transparent Bundling V1

## Status

This document defines a concrete v1 design for transparent small-file bundling
in `dcache_cp` and `dcache_mv`.

The full design is not implemented yet. The current codebase includes a
substantial end-to-end slice for upload plus transparent download:

- opt-in upload bundling via `--bundle-small-files`
- broader bottom-up packing within the requested upload transfer root
- transparent bundle-aware download unpack by default
- SquashFS bundle materialization via `mksquashfs` plus directory and
  bundle-object xattr metadata
- append-only upload reruns that reuse existing remote bundle objects and merge
  route generations safely
- bundled upload move with source deletion only after bundle commit succeeds
- bundled download move by marking logical bundle members deleted in metadata and
  removing bundle objects only after every member is retired
- append-only per-member deletion xattrs plus retrying route publication from
  current namespace state during bundle-backed download move
- bundle-aware `dcache_ls` sections by default, with `--no-bundles` as an
  opt-out while keeping the existing physical listing intact
- direct `dcache_ls` lookup of paths that exist only as logical bundled members
- sparse bundle-member reads via read-only `rclone mount` plus `sqfscat` when a
  small subset of members is requested, with automatic fallback to full bundle
  download otherwise
- Adler-32 verification of both downloaded bundle objects and extracted logical
  files
- current guardrails:
  upload bundling is still limited to a single recursive local source
  directory, with no `--file-list`
  true cross-process bundle-move publication still has no distributed
  compare-and-swap or lock in the namespace backend

## Problem

Many datasets contain directories with large numbers of small files. Copying and
staging those files individually is inefficient because:

- dCache staging works better with fewer, larger physical objects
- tape recall and metadata overhead dominate wall-clock time
- remote verification and retry cost scales with object count

At the same time, packing everything into one large archive is also undesirable
because:

- partial download becomes expensive
- unpacking becomes coarse-grained and operationally awkward
- move semantics become harder to reason about

The goal is therefore to bundle small files into moderately sized physical
objects, while keeping the user-facing copy behavior as logical-file oriented as
possible.

## Design Summary

V1 introduces an opt-in upload mode that bundles eligible small files into
anchor-local SquashFS archives near a target physical size, while keeping large
files as plain remote objects.

On download, packed content is detected automatically and unpacked transparently
into regular files on local disk.

Hot canonical metadata lives in dCache extended attributes on directories and
bundle objects, so the planner can resolve logical paths without staging a
manifest file from tape.

Each bundle also carries an embedded recovery manifest as a regular file at the
archive root, so lost xattrs can be reconstructed later without needing a
separate metadata object.

When only a small subset of members is requested and the required helper tools
are available, the current implementation can read those members directly from a
read-only `rclone mount` using `sqfscat`. If that sparse path is not available
or fails, download falls back to copying and verifying the full physical bundle
object before extraction.

## Why Hot Lookup Must Be Xattr-Based

The original design used an index file and manifest files as the canonical
bundle map. That is problematic on dCache because those metadata files are still
regular remote files and may themselves live on tape.

If resolving a logical file requires first staging a manifest file, the design
loses much of its value.

Therefore the hot lookup path must be namespace metadata, not file content.

- Directory xattrs can act as routing tables from logical paths to bundle IDs.
- Bundle-object xattrs can carry per-member metadata needed for logical
  verification after the correct bundle has been selected.
- Xattr reads stay in namespace metadata and should not require staging the
  bundle or a sidecar metadata file.
- Directory xattrs also make cross-subdirectory bundling possible because the
  anchor directory can publish metadata for an entire promoted subtree.
- An embedded manifest as a named root file remains useful for recovery, audit,
  and offline tooling, but it must not be required for the hot path.
- Multi-object commit can be handled by writing new routing shards under an
  inactive generation and flipping one active-generation xattr last.

Therefore:

- Directory xattrs and bundle-object xattrs are canonical for hot lookup.
- The embedded manifest is recovery metadata only.

## Scope

The implemented slice currently includes:

- transparent bundling on upload when explicitly enabled
- bottom-up promotion of small child subtrees into parent anchors within the
  requested transfer root
- transparent unpack on download when packed content is detected
- SquashFS bundle creation and extraction with embedded recovery manifest
- directory xattrs as route metadata and bundle-object xattrs as member metadata
- append-only upload resume/restart with remote bundle reuse and deprecation
  metadata on superseded bundle members
- bundled upload move with delete-after-commit semantics
- bundled download move with logical deletion metadata for bundle members and
  best-effort whole-bundle cleanup only after all members are retired
- append-only per-member deletion xattrs and retrying route publication from
  current namespace state during bundle-backed download move
- default bundle-aware `dcache_ls` sections that show logical members grouped
  under the bundle object path that backs them
- direct `dcache_ls` lookup of exact paths that exist only as logical bundled
  members
- sparse bundle-member reads through `rclone mount` plus `sqfscat` when a small
  subset of a SquashFS bundle is requested, with automatic fallback to the
  existing full-bundle download path
- Adler-32 verification of both physical bundle objects and extracted logical
  files during transparent download
- logical progress accounting for bundled uploads and bundled downloads

The remaining v1 design work includes:

- broader fault-injection coverage for cross-process bundle-backed download
  moves and whole-bundle cleanup; publication now retries from current state,
  but the backend still lacks a distributed compare-and-swap or lock
- optional polish around promotion thresholds, especially if a separate
  `bundle_promote_below` knob is still desired beyond `bundle_min_dir_total`
- optional tuning of the sparse-read heuristic and operator-facing diagnostics
- optional richer `dcache_ls` status display for deprecated or fully retired
  bundle objects

V1 excludes:

- automatic repacking of existing remote packed layouts in place
- bundling across unrelated anchor boundaries

## User-Facing CLI

### Upload-side opt-in

Bundling is enabled explicitly on upload and upload-move.

Implemented flags:

- `--bundle-small-files`
- `--bundle-format squashfs`
- `--bundle-target-size 1G`
- `--bundle-max-file-size 64M`
- `--bundle-min-dir-total 256M`
- `--bundle-max-members 10000`
- `--bundle-keep-temp` for debugging only

Recommended defaults for v1:

- `--bundle-format squashfs`
- `--bundle-target-size 1G`
- `--bundle-max-file-size 64M`
- `--bundle-min-dir-total 256M`
- `--bundle-max-members 10000`

The xattr shard codec is not user-configurable in v1. It is fixed to sorted TSV
records compressed with zlib and base64url-encoded for tool portability.

The SquashFS build parameters are also fixed in the current implementation:

- `-comp zstd`
- `-b 1M`
- `-no-duplicates`
- `-noappend`

SquashFS is the only supported bundle format.

### Download behavior

Download does not need an enable flag. If a packed layout is detected, unpacking
is automatic.

Current implementation notes:

- transparent unpack requires the dCache frontend API plus a bearer token when
  bundle metadata actually needs to be resolved
- `--file-list` downloads are supported for bundle-aware copy
- when only a small subset of members from a SquashFS bundle is requested, a
  sparse read is attempted via read-only `rclone mount` plus `sqfscat`; if that
  path is unavailable or fails, the code falls back to full bundle download and
  extraction
- dry-run output reports bundle object counts, covered logical file counts, and
  whether each planned bundle download would use sparse member reads or a full
  bundle read
- `--no-unpack-bundles` or `--ignore-bundle-xattrs` fall back to raw physical
  layout handling

Optional escape hatches:

- `--no-unpack-bundles` to download the raw `.dcpbundle` objects via the normal
  physical layout
- `--ignore-bundle-xattrs` for troubleshooting only

### Move behavior

Current implementation:

- bundled upload move deletes local bundled sources only after bundle objects,
  route-generation publication, and deprecation xattrs succeed
- bundled download move extracts and verifies requested members locally first,
  then records those logical members in append-only per-member deletion xattrs,
  republishes route metadata from the current namespace state until the stale
  references are gone, and only deletes the physical bundle object once every
  member is retired

Design target:

- the metadata-first delete path should remain conservative: hide logical files
  first, clean up physical bundle objects only after the namespace no longer
  routes to them

## Packing Domain

The packing domain in v1 is a bundle anchor subtree.

The current implementation performs bottom-up promotion of small child subtrees
into parent anchors, but it is bounded by the upload command's transfer root.

For example, with:

`dcache_cp filedir dcache:analysis/marc/filedir`

the planner may place bundle objects under `analysis/marc/filedir` or any of
its descendants, but it must not create a fresh packed layout rooted at
`analysis/marc` or `analysis`.

There is one sync-like exception to that rule: if active remote routing already
anchors a subtree under a parent directory above the requested transfer root,
reruns keep using that existing parent anchor for changed or new files in that
same subtree rather than splitting the packed layout.

- Small files may be bundled with files from child subdirectories of the same
  anchor subtree.
- A subtree may end up with zero, one, or multiple bundles.
- Large files anywhere inside that subtree remain plain remote files.
- Promotion is bottom-up and bounded by locality: v1 never combines files from
  unrelated branches outside a single anchor subtree.

This is still intentionally conservative. It allows useful cross-subdirectory
packing of tiny trees while avoiding coarse archive objects spanning unrelated
parts of the dataset.

## Bundle Selection Rules

A file is eligible for bundling only if all of the following hold:

- its size is less than or equal to `bundle_max_file_size`
- bundling is enabled for this transfer
- the file is a regular file

Whether an eligible file is actually packed is then decided by bottom-up anchor
planning.

- non-root anchors currently emit bundles only once the promoted eligible bytes
  in that anchor subtree reach `bundle_min_dir_total`
- otherwise the eligible remainder is promoted upward
- at the transfer root, undersized final bundles are allowed

The current implementation uses `bundle_min_dir_total` as the promotion
threshold as well as the non-root emission threshold. A separate
`bundle_promote_below` control is still optional future polish, not a required
part of the current code path. If it is ever added, the safest default would be
to keep it equal to `bundle_min_dir_total` and only diverge once real datasets
show that “promote tiny children upward” and “emit a bundle locally here” need
independent tuning.

Bundle planning is bottom-up.

1. Each directory first collects its own eligible direct files.
2. Each child subtree reports its eligible promoted remainder upward.
3. If a child subtree's remaining eligible bytes are below the current
  promotion threshold, that child remainder may be absorbed into the parent
  anchor.
4. The parent anchor then greedily fills bundles until either:

- estimated uncompressed bytes reach `bundle_target_size`, or
- member count reaches `bundle_max_members`

5. Any promoted remainder that still does not reach the target may continue
  upward, until the transfer root is reached.

6. At the transfer root, undersized final bundles are allowed.

Files larger than `bundle_max_file_size` remain plain files.

The split heuristic is based on uncompressed input size, not compressed size.
That is deliberate: access and unpack cost should remain bounded even when
SquashFS compresses extremely well.

In v1, bundling decisions are made from the current source tree during the file
scan. Existing remote packed state is not used to reshape bundle boundaries.
Existing remote content is only used later for reuse of exact unchanged bundle
objects and metadata.

The implemented sync path adds one more rule: existing active parent anchors may
be preserved on rerun, even above the current transfer root, when the remote
namespace already routes that subtree through that parent anchor.

## Remote Layout

For a logical anchor directory `/data/run42/`:

- plain large files remain at `/data/run42/<name>`
- packed bundle objects live under `/data/run42/.dcpacks/`
- hot lookup metadata lives in xattrs on `/data/run42/` itself

Proposed layout:

- `/data/run42/.dcpacks/bundles/<bundle_id>.dcpbundle`

The physical object path is format-independent. The actual bundle format is
published in bundle xattrs.

Bundle IDs must be deterministic from logical content, not sequence-based.
This keeps resume behavior stable and avoids churn when only one file changes.

## Hot Metadata Model

Hot lookup metadata is split into two layers:

- routing metadata on the anchor directory
- member metadata on the bundle object

This avoids any requirement to stage a manifest file just to answer “which
bundle contains logical path X?”.

### Definition: anchor directory

An anchor directory is the namespace directory that owns bundled metadata for a
subtree.

- its routing xattrs describe which logical files in that subtree belong to
  which bundle
- its `.dcpacks/bundles/` directory stores the physical bundle objects for that
  subtree
- a file's anchor directory may be its direct parent, or some ancestor if the
  file was promoted upward during bottom-up packing

So the anchor is not “the directory that contains the file” and not “the
directory that contains the bundle” as two separate concepts. It is the logical
owner of the bundle set for that subtree, and the bundle objects are stored
under that same directory.

### Anchor-directory routing xattrs

The anchor directory publishes the routing table for bundled content in its
subtree.

Suggested keys:

- `dcache_cp.bundle_anchor.schema=v1`
- `dcache_cp.bundle_anchor.active_generation=<generation>`
- `dcache_cp.bundle_anchor.route.codec=tsv+zlib+base64url`
- `dcache_cp.bundle_anchor.route.<generation>.shards=<count>`
- `dcache_cp.bundle_anchor.route.<generation>.<shard_id>=<encoded payload>`

Routing records map logical relative paths within the anchor subtree to a bundle
ID.

The physical bundle object path is deterministic:

`<anchor_dir>/.dcpacks/bundles/<bundle_id>.dcpbundle`

This avoids repeating object paths inside every route record.

Suggested logical record shape:

```text
relative/path/to/file.tsv\t<bundle_id>
subdir/other.txt\t<bundle_id>
```

Shards are required because xattr payload sizes are deployment-dependent.

Shard payloads are UTF-8 text after base64url encoding of zlib-compressed TSV.
This keeps the values compact while still working cleanly with CLI tools and
protocol stacks that are text-biased.

The active-generation pointer is the commit switch. Readers load only the
generation named by `dcache_cp.bundle_anchor.active_generation`.

### Bundle-object member xattrs

Each bundle object publishes the member metadata needed for logical verification
and extraction planning.

Suggested keys on the bundle object:

- `dcache_cp.bundle.schema=v1`
- `dcache_cp.bundle.id=<bundle_id>`
- `dcache_cp.bundle.anchor_dir=data/run42`
- `dcache_cp.bundle.format=squashfs`
- `dcache_cp.bundle.generation=<generation>`
- `dcache_cp.bundle.created_at=<timestamp>`
- `dcache_cp.bundle.members.codec=tsv+zlib+base64url`
- `dcache_cp.bundle.members.shards=<count>`
- `dcache_cp.bundle.members.<shard_id>=<encoded payload>`
- `dcache_cp.bundle.manifest_entry=._dcache_cp_manifest.v1.json`

Suggested logical member record shape:

```text
relative/path/to/file.tsv\t00ab12cd\t12345\t1713690000000000000\t0644
```

The per-bundle xattrs are what make a per-bundle `getxattr` useful: once the
directory routing xattrs have identified the bundle ID, the planner can read the
bundle object's own xattrs to get per-member Adler-32, size, and mtime without
touching any tape-backed metadata file.

## Embedded Manifest Recovery

Each bundle contains an embedded manifest as a regular file at the archive root.

Suggested entry name:

`._dcache_cp_manifest.v1.json`

This embedded manifest is not used for the hot path. Its purpose is recovery:

- rebuilding lost bundle-object xattrs
- rebuilding lost anchor-directory routing xattrs
- offline inspection of bundle contents

For SquashFS, the manifest can be extracted directly by pathname with
`unsquashfs` without unpacking every archive member. The current download path
still copies the whole bundle object first, but the archive layout itself is now
compatible with future range-aware or mounted access.

Suggested shape:

```json
{
  "schema": "dcache_cp.bundle_manifest.v1",
  "bundle_id": "<bundle_id>",
  "anchor_dir": "data/run42",
  "generation": "<generation>",
  "created_at": "2026-04-21T12:00:00Z",
  "format": "squashfs",
  "bundle_object": ".dcpacks/bundles/<bundle_id>.dcpbundle",
  "bundle_adler32": "1a2b3c4d",
  "bundle_size": 1073741824,
  "squashfs": {
    "compressor": "zstd",
    "block_size": 1048576,
    "no_duplicates": true
  },
  "members": [
    {
      "path": "sample_a.tsv",
      "size": 12345,
      "adler32": "00ab12cd",
      "mtime_ns": 1713690000000000000,
      "mode": "0644"
    }
  ]
}
```

The embedded manifest duplicates data that is also present in xattrs. That is
acceptable because:

- xattr lookup stays fast for planning
- the embedded manifest remains self-contained for recovery and validation
- the hot path no longer depends on any separate metadata object

## Xattr Usage

Xattrs are written after the physical object exists and has been verified.

Directory xattrs are the routing layer. Bundle-object xattrs are the member
metadata layer.

### Directory xattrs

Suggested keys on the anchor directory:

- `dcache_cp.bundle_anchor.schema=v1`
- `dcache_cp.bundle_anchor.active_generation=<generation>`
- `dcache_cp.bundle_anchor.route.codec=tsv+zlib+base64url`
- `dcache_cp.bundle_anchor.route.<generation>.shards=<count>`
- `dcache_cp.bundle_anchor.route.<generation>.<shard_id>=<routing payload>`

These xattrs are the authoritative path-to-bundle routing map for the subtree
anchored at that directory.

### Bundle object xattrs

Suggested keys on the bundle object:

- `dcache_cp.bundle.schema=v1`
- `dcache_cp.bundle.id=<bundle_id>`
- `dcache_cp.bundle.anchor_dir=data/run42`
- `dcache_cp.bundle.format=squashfs`
- `dcache_cp.bundle.generation=<generation>`
- `dcache_cp.bundle.created_at=<timestamp>`
- `dcache_cp.bundle.members.codec=tsv+zlib+base64url`
- `dcache_cp.bundle.members.shards=<count>`
- `dcache_cp.bundle.members.<shard_id>=<member payload>`
- `dcache_cp.bundle.manifest_entry=._dcache_cp_manifest.v1.json`
- `dcache_cp.bundle.members.count=<count>`
- `dcache_cp.bundle.squashfs.compressor=zstd` for SquashFS bundles
- `dcache_cp.bundle.squashfs.block_size=1048576` for SquashFS bundles
- `dcache_cp.bundle.squashfs.no_duplicates=true` for SquashFS bundles

### Why these xattrs are useful

- operators can use `--findxattr` to locate packed directories or bundle objects
- planners can resolve logical paths to bundle IDs without staging a metadata
  file from tape
- repair tooling can rediscover bundle metadata even if route xattrs or
  bundle-member xattrs are missing or stale
- debugging is easier because a bundle object is self-describing

### Why xattrs are not enough on their own

- they have deployment-dependent size limits, so sharding is required
- they are not ideal for external audit or offline tooling
- multi-xattr publish still needs a generation-flip commit protocol

## Upload Behavior

### Planning

The logical upload plan is first built exactly as today.

When bundling is enabled:

- plain large-file entries remain plain physical objects
- eligible small-file entries are grouped by anchor subtree
- child remainders smaller than the current `bundle_min_dir_total` threshold
  may be promoted into the parent anchor
- each anchor subtree is split into one or more bundle specs

### Build phase

Each bundle is created locally in a temporary working directory.

The archive member paths are relative to the logical anchor directory.

For SquashFS bundles, the builder populates a temporary directory tree and then
invokes `mksquashfs` with fixed settings.

During bundle creation, the implementation computes and records:

- per-member size
- per-member Adler-32
- per-member mode and mtime

### Upload and verification

Each physical object is uploaded and verified via the existing transport model.

That means:

- bundle object gets local Adler-32 and remote Adler-32 verification
- bundle-object xattrs are written only after the bundle object has been
  verified
- anchor-directory routing xattrs are published only after all referenced bundle
  objects are verified

### Commit order

Commit must be write-last on the anchor directory's active route-generation
xattr.

Recommended order:

1. upload bundle objects
2. write bundle-object xattrs
3. write new routing shards under an inactive generation on the anchor
  directory
4. flip `dcache_cp.bundle_anchor.active_generation` last
5. optionally garbage-collect obsolete bundles and old
  route generations

If the process crashes before step 4, the uploaded bundles are orphaned but not
logically visible. That is acceptable for v1.

### Move-upload semantics

Local source files are deleted only after the bundle commit has completed and the
active route generation is in place.

Deletion never happens immediately after archive creation.

## Download Behavior

### Detection

Download planning checks directory xattrs for a bundle anchor.

For direct path lookup, the planner walks upward through parent directories
until it finds `dcache_cp.bundle_anchor.schema`.

For recursive planning of an anchor subtree, the planner loads the active route
generation and all of its routing shards from that anchor directory.

If anchor xattrs are present:

- the active route shards are loaded from namespace metadata
- bundle members are exposed as logical files
- bundle internals are hidden from normal logical planning

Plain large files and logical bundled files can coexist in the same directory.

### Logical planning

The planner maps requested logical files to two groups:

- plain physical files
- bundle-backed logical files keyed by bundle ID

Bundle downloads are deduplicated. If ten requested logical files belong to the
same bundle, that bundle is downloaded only once.

After routing xattrs identify the bundle object, the planner reads that bundle
object's member xattrs to obtain per-member Adler-32, size, and mtime.

### Physical download and verification

When sparse extraction is not used, the physical bundle object is downloaded to
a temporary local file and verified with remote Adler-32, exactly like a normal
file download.

### Extraction and logical verification

Only after physical verification succeeds:

- the requested archive members are extracted into temporary local files
- each requested member is Adler-32 checked against bundle-object xattrs
- each verified member is atomically moved into its final destination path

For SquashFS bundles, the current implementation uses one of two paths:

- sparse path: read-only `rclone mount` plus `sqfscat` for only the requested
  members when the request covers a small enough subset of the bundle and the
  required tools are available
- fallback path: download the full bundle object locally, verify it, then use
  `unsquashfs` to extract only the requested members by path

The user must never see a final local path that has not passed logical member
verification.

### Skip-verified behavior

Skip-verified on download works at logical member level.

If a logical target file already exists locally and matches the bundle-member Adler-32,
that member is skipped.

In v1 the authoritative comparison source for this step is bundle-object xattrs,
not the embedded recovery manifest.

If all requested members in a bundle are already verified locally, that bundle is
not downloaded.

## Move-Download Semantics

Plain remote files behave exactly as today.

Bundle-backed logical files use logical deletion, not in-place bundle rewrite.

Supported in the current implementation:

- moving any requested logical subset of a bundle after successful local
  extraction and verification
- rerunning that move safely when local files already exist and match, because
  the remote metadata commit is separate from the physical download step

The deletion model is:

1. materialize the requested logical members, either by sparse mounted reads or
  by downloading and verifying the full physical bundle object, unless
  `--skip-verified` already proves every requested local member is present and
  correct
2. extract the requested logical members and verify them against bundle-object
   member xattrs
3. atomically publish the verified local files
4. write append-only per-member deletion metadata on the bundle object
5. publish a new anchor route generation that removes those logical members,
  retrying from freshly read namespace state until stale routed references are
  gone or the operation fails closed
6. only if the bundle now has no active routed members left and every member is
   either deleted or deprecated, attempt best-effort deletion of the physical
   bundle object

This keeps the dangerous operation last.

If the process crashes after step 4 but before step 5, readers that consult the
bundle object's deleted-member metadata must still suppress those moved members,
even if the older route generation still references them. That avoids accidental
logical resurrection on rerun or listing.

Reasoning:

- deleting the physical bundle object too early would remove unrelated logical
  files
- rewriting a bundle in place remains out of scope
- metadata-first logical deletion gives restart-safe move semantics without
  needing remote repacking

## Resume And Recovery

### Stable identity

Bundle IDs are deterministic from logical member metadata and content. They must
not depend on row order or worker order.

### Upload resume

If a bundle object already exists remotely with matching checksum and expected
bundle-object xattrs, it can be reused and skipped.

Route-generation publication remains the final commit step.

### Existing remote state and sync-like reruns

V1 does not attempt in-place mutation or partial merge of existing remote
bundles.

The planner decides bundle boundaries from the current source tree during the
scan.

Then:

- unchanged bundles are reused if their deterministic bundle ID already exists
  remotely with matching checksum and expected xattrs
- changed content causes new bundle IDs and new bundle objects to be written
- older bundle objects are left in place; superseded members are marked via
  deprecation metadata after the new route generation has been published
- members moved out of a bundle are marked with deletion metadata on that bundle
  object, and stale route entries must ignore those deleted members until the
  new route generation is active
- reruns preserve an already-active parent anchor for a subtree rather than
  forcing that subtree back under the current transfer root

If a logical file exists in both an old and a new bundle during a transition,
the active route generation is authoritative.

If route metadata is lost and must be rebuilt by scanning bundles, the highest
bundle generation wins for duplicate logical paths. Ties are broken by newest
`dcache_cp.bundle.created_at`, then by lexical bundle ID.

This keeps reruns and future sync-like behavior deterministic without requiring
remote bundle surgery.

### Download resume

Verified local logical members are skipped.

Temporary bundle downloads and temporary extracted files are not reused in v1.
They are scratch state only.

## Checksum Model

There are two verification layers.

### Physical verification

For each remote object that is actually copied:

- compute local Adler-32
- fetch remote Adler-32 with `ada --checksum`
- require equality

This covers:

- plain files
- bundle objects

### Logical verification

For each extracted member from a bundle:

- compute local Adler-32 after extraction
- compare against the member Adler-32 stored in bundle-object xattrs
- publish the file only after equality

This guarantees end-to-end integrity even though the logical file is transported
inside a bundled archive object.

## Interactions With Staging

The current staging pipeline works on physical remote objects. That remains true.

For packed content this is an advantage:

- fewer physical objects are staged
- bundles are already near the desired object size
- the existing staging batch logic remains useful

Logical files inside the same bundle naturally share the same staged object.

## Interactions With Existing Code

This feature should be implemented as a planning and post-processing layer on top
of the existing verified physical transfer engine.

The existing transport component should continue to treat every copied object as
just a physical file with Adler-32 verification.

That separation is important:

- physical copy logic stays simple
- bundle awareness lives in planners, temp builders, and extractors
- retry and checksum semantics stay aligned with current behavior

## Failure Model

Expected failure behavior in v1:

- interrupted upload before route-generation publication leaves orphaned bundle
  objects but no visible logical packed state
- interrupted download leaves only temporary local files, never final verified
  targets
- interrupted move never deletes source before verified local publication
- corrupted bundle object fails at physical Adler-32 verification
- corrupted extracted member fails at logical bundle-member Adler-32
  verification

## Test Matrix

Minimum test coverage for implementation:

- small files in one directory become one bundle
- many small files in one directory split into multiple bundles near target size
- mixed small and large files keep large files plain
- small child subtrees can be promoted into a parent anchor when below the
  current promotion threshold
- recursive upload can bundle across subdirectory boundaries within one anchor
  subtree
- promoted bundling never creates a fresh anchor above the requested transfer
  root
- sync reruns may continue using an existing parent anchor above that transfer
  root when that subtree is already routed there
- SquashFS bundle creation uses the configured fixed build settings and embeds
  the recovery manifest at archive root
- unsupported bundle formats are rejected during planning and extraction
- bundle upload verifies bundle checksum and publishes xattrs only after success
- the embedded manifest is written into the archive and is recoverable by name
- bundle download verifies physical object then logical members
- bundle download uses sparse member reads when only a small subset of a
  SquashFS bundle is requested and the required helper tools are available,
  with fallback to full bundle download otherwise
- recursive planning resolves bundled files from directory xattrs, not from a
  separate staged metadata object
- verified local member skip avoids bundle download when no requested members are
  needed
- existing local files remain intact on bundle mismatch or extraction failure
- upload move deletes local sources only after bundle commit
- download move marks requested members deleted in metadata and only removes the
  physical bundle once every member is retired
- bundle move tombstones are stored as append-only per-member xattrs and stale
  routed references are retried away from current namespace state
- xattrs are written on anchor directories and bundle objects after commit
- `dcache_ls` shows bundle-aware sections by default and can resolve exact
  bundled logical-member paths that do not exist as physical namespace entries

## dcache_ls Plan

Bundle-aware listing is now the default behavior in `dcache_ls`. Use
`--no-bundles` to suppress it.

Current behavior:

- keep the existing physical `ada --stat` listing as the primary view
- append a `bundles:` section for each listed directory in both long and
  non-long output modes
- group logical bundled members by physical bundle object path
- hide members that have deletion metadata, even if a stale route generation
  still references them
- show inherited parent-anchor bundle paths relative to the current directory,
  for example `../.dcpacks/bundles/...`
- if `dcache_ls` is called on an exact path that does not exist physically, try
  resolving it as a logical bundled member before reporting it missing

Short-format bundle output is intentionally compact and colorizes packed logical
member names so they stand out from the physical listing without replacing it.

Remaining polish:

- consider richer status display for deprecated or fully retired bundle objects
- decide whether additional bundle-specific summary counters or health markers
  are useful in long format

Implemented design choices:

1. Keep the existing physical `ada --stat` listing as the base view.
2. Make bundle-aware overlay output the default, with `--no-bundles` as the
   escape hatch.
3. Resolve active anchor routes affecting the listed directory by reading the
   directory's own xattrs and any nearest parent anchor whose routes cover that
   subtree.
4. Group routed logical members by physical bundle object path.
5. Allow exact-path lookup of bundled logical members that do not exist as
   standalone namespace entries.

Suggested rendering shape:

```text
/analysis/marc/filedir:
drwxr-xr-x ... sub/
-rw-r--r-- ... plain.txt

bundles:
  .dcpacks/bundles/8f2b...dcpbundle
    file-a.txt        12KiB  0644  adler=00ab12cd
    file-b.txt        18KiB  0644  adler=91ef3344
  ../.dcpacks/bundles/1c77...dcpbundle
    inherited.txt      4KiB  0644  adler=00112233
```

Rendering rules:

- non-recursive listing should show only logical files whose parent directory is
  the directory currently being listed
- recursive listing should emit bundle blocks separately for each directory
  section, so the view stays local and tree-like instead of dumping an entire
  anchor subtree into one block
- each logical member row should show at least relative name, size, mode,
  mtime, and Adler-32 from bundle-object xattrs
- the bundle heading should show the physical bundle object location relative to
  the listed directory, so users can see whether membership comes from the
  local anchor or an inherited parent anchor

Implementation steps inside `dcache_ls`:

1. factor the current listing code so a directory section can accept synthetic
   bundle-member rows alongside physical rows
2. reuse the same xattr decoding helpers already used by `dcache_cp` download
   planning
3. add a resolver that can map one listed directory to:
   - local anchor-owned members
   - inherited members from the nearest parent anchor, when relevant
4. render bundle groups after the normal physical rows, not interleaved with
   them, so physical namespace layout and logical packed membership are both
   visible
5. degrade cleanly: if API or bearer-token resolution fails, keep the normal
   physical listing and print one warning instead of failing the whole command

Current automated coverage includes:

- one anchor-local bundle appears under the listed directory with its direct
  logical children
- a directory with an inherited parent anchor shows that parent bundle path and
  only the logical members belonging to the current folder
- recursive listing prints separate bundle blocks per directory section
- raw physical listing remains unchanged when `--bundles` is not requested
- missing xattr access degrades to the current physical-only listing

Additional coverage still worth adding:

- direct bundled-member path lookup in `dcache_ls`
- warnings and degraded behavior when bundle xattrs disappear mid-listing
- concurrent bundle-backed move scenarios across separate processes

## V1 Recommendation

Proceed with this exact model:

- canonical hot metadata in anchor-directory and bundle-object xattrs
- embedded manifest as a named root file inside the archive for recovery only
- bundling within an anchor subtree, with bottom-up promotion of small child
  subtrees into the parent when appropriate
- SquashFS as the only supported bundle format
- automatic unpack on download
- metadata-first logical deletion for bundle-backed download move
- future option to exploit SquashFS random-access-friendly layout through a
  mounted or range-aware remote access path when only a few members are needed

This is the smallest design that preserves transparent UX, verified copy
semantics, restart safety, and dCache staging benefits without turning the remote
layout into an opaque or fragile system.