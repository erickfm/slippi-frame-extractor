# slippi-frame-extractor

A fixed-schema pipeline that converts Slippi replays into per-frame Parquet datasets, with separate P1/P2 views for next-input-prediction models.


## Features

- **Frame-by-frame extraction**
  Parses `.slp` replays using libmelee.

- **Fixed column schema**
  - `MAX_PROJ = 8` projectile slots
  - Pre-seeded Nana (Ice Climbers partner) fields for consistent datatypes
  Ensures every replay produces identical columns, even if no projectile or Nana data is present.

- **Two player-centric outputs**
  - `*-p1.parquet`: Port 1 → `self_*`, Port 2 → `opp_*`
  - `*-p2.parquet`: Port 2 → `self_*`, Port 1 → `opp_*` (mirrored)
  This doubles your training data for free — every game gives you both perspectives.

- **No mixed types**
  Every column is a single consistent type across all rows and all replays. No string/int mixing, no type coercion surprises. See [Data Types](#data-types) for details.

- **Batch processing with error handling**
  Corrupt or malformed `.slp` files are logged and skipped — one bad file won't kill a long batch run.

- **Self-describing filenames**
  Format: `<stage>_<p1char>_vs_<p2char>_<timestamp>_<uuid>-p{1,2}.parquet`


## Quick Start

```bash
pip install pandas pyarrow melee
python extract.py /path/to/slp/files -o /path/to/output
```

Result: two Parquet files per replay.

```
final_destination_fox_vs_marth_2025_06_01t15_30_45z_abcd1234-p1.parquet
final_destination_fox_vs_marth_2025_06_01t15_30_45z_abcd1234-p2.parquet
```

Load with pandas:
```python
import pandas as pd
df = pd.read_parquet("...-p1.parquet")
```


## Column Schema

Each row is one game frame. All replays produce the same columns regardless of characters or projectiles present.

| Group | Columns | Description |
|-------|---------|-------------|
| **Frame metadata** | ~20 | `frame`, `distance`, `stage`, blastzones, edge positions, platform positions, randall position |
| **Player state** × 2 | ~50 × 2 | Character, action state, controller inputs, position, velocity, percent, stocks, shield, ECB, etc. |
| **Projectiles** (8 slots × 8 fields) | 64 | Frame, owner, position, velocity, subtype, type for each of 8 possible active projectiles |
| **Nana state** × 2 | ~48 × 2 | Ice Climbers partner data, pre-seeded with defaults when ICs aren't in the game |

### Data Types

Every column has a single consistent type. There is no mixing of strings and ints, no mixed numeric types within a column. This matters for model training — pandas and PyTorch/NumPy will not silently upcast or create object columns.

| Type | Columns | Empty/missing sentinel |
|------|---------|----------------------|
| **int** | `frame`, `stage`, `port`, `character`, `action`, `action_frame`, `costume`, `stock`, `jumps_left`, `hitlag_left`, `hitstun_left`, `invuln_left`, all `proj*_frame`, `proj*_owner`, `proj*_subtype`, `proj*_type` | `-1` |
| **bool** | All `btn_*` columns, `facing`, `invulnerable`, `moonwalkwarning`, `off_stage`, `on_ground` | `False` |
| **float32** | `distance`, sticks (`main_x/y`, `c_x/y`), shoulders (`l_shldr`, `r_shldr`), ECBs, `pos_x/y`, all `speed_*`, `percent`, `shield_strength`, all `proj*_pos_*`, `proj*_speed_*`, stage geometry, randall | `NaN` |

All categoricals (stage, character, action, projectile type) are stored as their native integer enum values from libmelee — no string-to-int lookup maps involved. This means the integer codes come directly from the game engine via `melee.enums` and match the values in [hohav/ssbm-data](https://github.com/hohav/ssbm-data).

### Stage data

Stage geometry (blastzones, edge positions, platform positions) is static per game — the same values repeat on every frame. The one exception is **Randall** (the moving cloud on Yoshi's Story), which changes position each frame. This redundancy is intentional: it keeps each frame self-contained so you can shuffle/sample rows freely without needing to join against a separate metadata table.

If storage is a concern, these columns compress extremely well in Parquet (identical values per file = near-zero overhead with columnar compression).

### Controller inputs

Inputs are stored as raw values from libmelee with no transformation:

- **Buttons**: Individual boolean columns (`btn_BUTTON_A`, `btn_BUTTON_B`, etc.) — `True`/`False` per frame
- **Analog sticks**: `main_x`, `main_y`, `c_x`, `c_y` — float32 values, typically in [-1.0, 1.0]
- **Triggers**: `l_shldr`, `r_shldr` — float32 analog values

For model training you'll likely want to transform these downstream (e.g., quantize stick positions into discrete zones, bitpack buttons into a single int, combine into action tokens). That kind of opinionated preprocessing belongs in your training pipeline, not here — this repo outputs the raw source of truth.

### Categorical encoding

All categoricals use the integer `.value` from their libmelee enum directly:

- **Stages**: e.g., `FINAL_DESTINATION` → 25, `BATTLEFIELD` → 24
- **Characters**: e.g., `FOX` → 1, `MARTH` → 18
- **Actions**: e.g., `STANDING` → 14, `DASHING` → 20
- **Projectile types**: e.g., `FOX_LASER` → 54, `MARIO_FIREBALL` → 48

Unknown or missing values are encoded as `-1`.

**Action state IDs are character-scoped.** Melee reuses action IDs across characters — for example, action 351 means `FOX_ILLUSION` for Fox/Falco but `SWORD_DANCE_2_MID` for Marth. The `(character, action)` pair is unambiguous. Consult `melee.enums` for the full mappings of all categorical values.

### Perspective columns

In the output files, player columns are renamed for symmetry:

| Raw column | In `-p1.parquet` | In `-p2.parquet` |
|------------|-----------------|-----------------|
| `p1_percent` | `self_percent` | `opp_percent` |
| `p2_percent` | `opp_percent` | `self_percent` |

This means a model trained on `self_*`/`opp_*` columns works from either player's point of view without any code changes.


## Notes for Model Training

- **Frame rate**: Melee runs at 60fps. Each row is one frame (~16.67ms). A typical 4-minute game is ~14,400 frames.
- **Action state**: The `action` column is often the most informative single feature — it tells you exactly what the character is doing (attacking, shielding, in hitstun, etc.). The `action_frame` tells you how many frames into that action the character is. Remember that action IDs are character-scoped, so always pair with `character` for unambiguous interpretation.
- **Distance**: `distance` is the Euclidean distance between the two players, pre-computed by libmelee.
- **ECB (Environmental Collision Box)**: The four ECB points (bottom, left, right, top) define the character's collision diamond. Useful for understanding hurtbox positioning but may be noisy for input prediction.
- **Projectiles**: Most frames have 0 active projectiles. The 8-slot system is a worst-case cap. Empty slots use `-1` (ints) or `NaN` (floats). Projectile slots are unordered — the same projectile may appear in different slots across frames.
- **Nana columns**: Only populated when Ice Climbers are in the game. Otherwise all defaults (`False`, `-1`, `NaN`). You may want to drop these entirely if your dataset doesn't include ICs.
- **Port ordering**: Players are assigned `p1_`/`p2_` by iteration order over ports. This doesn't necessarily correspond to "player 1" in the game UI — but since the perspective files normalize to `self_`/`opp_`, it doesn't matter for training.
- **Costume**: Static per game (doesn't change between frames). Unlikely to be useful for input prediction but is included for completeness.
- **No one-hot encoding**: Categoricals (stage, character, action, projectile type) are stored as plain integers, not one-hot vectors. This keeps the output compact and unopinionated — one-hot encoding, learned embeddings, or any other representation is a modeling decision that belongs in your training pipeline. Buttons are already binary (one column per button), which is effectively one-hot by nature.
- **No strings in the output**: Every column is int, float32, or bool. You can load the parquet directly into a tensor without dtype conversion beyond what your framework needs.
- **Shuffling**: Because each row is self-contained (stage geometry is repeated, not referenced), you can freely shuffle rows across files for training without losing context. Sequence models should preserve frame order within a game.


## Dependencies

```bash
pip install pandas pyarrow melee
```

- **pandas**: DataFrame manipulation
- **pyarrow**: Parquet I/O
- **melee** (libmelee): Melee-specific enums, game-state parsing, stage geometry


## License

Released under the MIT License. Slippi replay files remain the user's property.
