"""
Batch-process all Slippi .slp files in a directory, dumping each replay to
two parquet tables (p1 & p2 perspective) with a fixed column schema.

Files produced for each replay ( <base> = <stage>_<p1>_vs_<p2>_<timestamp>_<uuid> ):

  • <base>-p1.parquet
  • <base>-p2.parquet
"""

import os
import re
import uuid
import unicodedata
import pathlib
import pandas as pd
from melee import Console
from melee.enums import Menu

# ----------------------------------------------------------------------------
# Import static category maps so we only store ints in the DataFrame
from cat_maps import STAGE_MAP, CHARACTER_MAP, PROJECTILE_TYPE_MAP, ACTION_MAP

SLP_DIR     = "/Volumes/slippi/ranked-anonymized"
OUT_DIR     = pathlib.Path("/Volumes/slippi/extracted")        # Change this for a different output folder
MAX_PROJ    = 8                        # Hard cap on projectile slots

# ----------------------------------------------------------------------------
def slug(s: str | None) -> str:
    """Lower-case, de-accent, replace spaces/unsafe chars with '_'."""
    if not s:
        return "unknown"
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    s = re.sub(r"[^\w\s-]", "", s.lower()).strip()
    return re.sub(r"[-\s]+", "_", s)

# ----------------------------------------------------------------------------
def encode_row(row: dict) -> dict:
    """
    Overwrite any string‐valued categoricals in `row` with their integer codes.
    Unknown or missing labels → -1. After this, only ints/floats/bools remain.
    """
    # 1) Stage → int
    row["stage"] = STAGE_MAP.get(row.get("stage"), -1)

    # 2) p1_/p2_ character, action, and nana_character, nana_action → int
    for prefix in ("p1_", "p2_"):
        row[f"{prefix}character"]     = CHARACTER_MAP.get(row.get(f"{prefix}character"), -1)
        row[f"{prefix}action"]        = ACTION_MAP.get(row.get(f"{prefix}action"), -1)
        row[f"{prefix}nana_character"] = CHARACTER_MAP.get(row.get(f"{prefix}nana_character"), -1)
        row[f"{prefix}nana_action"]    = ACTION_MAP.get(row.get(f"{prefix}nana_action"), -1)

    # 3) projX_type → int
    for j in range(MAX_PROJ):
        key = f"proj{j}_type"
        row[key] = PROJECTILE_TYPE_MAP.get(row.get(key), -1)

    return row

# List all files in the directory (reverse order so newest first)
files = os.listdir(SLP_DIR)

for file in files:
    if not file.lower().endswith(".slp"):
        continue

    SLP_PATH = f"{SLP_DIR}/{file}"
    console = Console(is_dolphin=False, path=SLP_PATH, allow_old_version=True)
    console.connect()

    rows = []
    first_stage = first_p1_char = first_p2_char = timestamp = None

    while True:
        gs = console.step()
        if gs is None:
            break
        if gs.menu_state != Menu.IN_GAME:
            continue

        # ------------------------------------------------ frame‐level ----------
        if first_stage is None:
            first_stage   = gs.stage.name if gs.stage else "unknown_stage"
            timestamp     = gs.startAt  # e.g., '2023-05-02T03:39:34Z'

        row = {
            "frame":    gs.frame,
            "distance": gs.distance,
            "stage":    gs.stage.name if gs.stage else None,
            "startAt":  gs.startAt,
        }

        # ------------------------------------------------ players -------------
        for idx, (port, ps) in enumerate(gs.players.items()):
            pref = f"p{idx+1}_"

            if idx == 0 and first_p1_char is None:
                first_p1_char = ps.character.name
            if idx == 1 and first_p2_char is None:
                first_p2_char = ps.character.name

            row[f"{pref}port"]         = port
            row[f"{pref}character"]    = ps.character.name
            row[f"{pref}action"]       = ps.action.name
            row[f"{pref}action_frame"] = ps.action_frame

            # buttons
            for btn, state in ps.controller_state.button.items():
                row[f"{pref}btn_{btn.name}"] = state

            # sticks & shoulders
            # These will be floats (default Python floats → float64 initially)
            row[f"{pref}main_x"], row[f"{pref}main_y"] = ps.controller_state.main_stick
            row[f"{pref}c_x"],    row[f"{pref}c_y"]    = ps.controller_state.c_stick
            row[f"{pref}l_shldr"] = ps.controller_state.l_shoulder
            row[f"{pref}r_shldr"] = ps.controller_state.r_shoulder

            # misc scalars (including ECB split into x/y). ECBs are Python floats here.
            row.update({
                f"{pref}costume":              ps.costume,                   # int
                f"{pref}ecb_bottom_x":         float(ps.ecb_bottom[0]),     # ensure Python float
                f"{pref}ecb_bottom_y":         float(ps.ecb_bottom[1]),
                f"{pref}ecb_left_x":           float(ps.ecb_left[0]),
                f"{pref}ecb_left_y":           float(ps.ecb_left[1]),
                f"{pref}ecb_right_x":          float(ps.ecb_right[0]),
                f"{pref}ecb_right_y":          float(ps.ecb_right[1]),
                f"{pref}ecb_top_x":            float(ps.ecb_top[0]),
                f"{pref}ecb_top_y":            float(ps.ecb_top[1]),
                f"{pref}facing":               ps.facing,                   # bool
                f"{pref}hitlag_left":          ps.hitlag_left,              # int
                f"{pref}hitstun_left":         ps.hitstun_frames_left,      # int
                f"{pref}invuln_left":          ps.invulnerability_left,      # int
                f"{pref}invulnerable":         ps.invulnerable,             # bool
                f"{pref}jumps_left":           ps.jumps_left,               # int
                f"{pref}moonwalkwarning":      ps.moonwalkwarning,          # bool
                f"{pref}off_stage":            ps.off_stage,                # bool
                f"{pref}on_ground":            ps.on_ground,                # bool
                f"{pref}percent":              float(ps.percent),           # float
                f"{pref}pos_x":                float(ps.position.x),         # float
                f"{pref}pos_y":                float(ps.position.y),         # float
                f"{pref}shield_strength":      float(ps.shield_strength),    # float
                f"{pref}speed_air_x_self":     float(ps.speed_air_x_self),   # float
                f"{pref}speed_ground_x_self":  float(ps.speed_ground_x_self),# float
                f"{pref}speed_x_attack":       float(ps.speed_x_attack),     # float
                f"{pref}speed_y_attack":       float(ps.speed_y_attack),     # float
                f"{pref}speed_y_self":         float(ps.speed_y_self),       # float
                f"{pref}stock":                ps.stock,                    # int
            })

            # ------------------------------------------------ Nana -------------
            nana = ps.nana
            nana_pref = f"{pref}nana_"
            if nana:
                row.update({
                    f"{nana_pref}character":     nana.character.name,        # str
                    f"{nana_pref}action":        nana.action.name,           # str
                    f"{nana_pref}action_frame":  nana.action_frame,          # int
                })
                for btn, state in nana.controller_state.button.items():
                    row[f"{nana_pref}btn_{btn.name}"] = state             # bool
                row[f"{nana_pref}main_x"], row[f"{nana_pref}main_y"] = nana.controller_state.main_stick
                row[f"{nana_pref}c_x"],    row[f"{nana_pref}c_y"]    = nana.controller_state.c_stick
                row[f"{nana_pref}l_shldr"] = nana.controller_state.l_shoulder
                row[f"{nana_pref}r_shldr"] = nana.controller_state.r_shoulder

                row.update({
                    f"{nana_pref}costume":            nana.costume,                   # int
                    f"{nana_pref}ecb_bottom_x":       float(nana.ecb_bottom[0]),      # float
                    f"{nana_pref}ecb_bottom_y":       float(nana.ecb_bottom[1]),
                    f"{nana_pref}ecb_left_x":         float(nana.ecb_left[0]),
                    f"{nana_pref}ecb_left_y":         float(nana.ecb_left[1]),
                    f"{nana_pref}ecb_right_x":        float(nana.ecb_right[0]),
                    f"{nana_pref}ecb_right_y":        float(nana.ecb_right[1]),
                    f"{nana_pref}ecb_top_x":          float(nana.ecb_top[0]),
                    f"{nana_pref}ecb_top_y":          float(nana.ecb_top[1]),
                    f"{nana_pref}facing":             nana.facing,                   # bool
                    f"{nana_pref}hitlag_left":        nana.hitlag_left,              # int
                    f"{nana_pref}hitstun_left":       nana.hitstun_frames_left,      # int
                    f"{nana_pref}invuln_left":        nana.invulnerability_left,      # int
                    f"{nana_pref}invulnerable":       nana.invulnerable,             # bool
                    f"{nana_pref}jumps_left":         nana.jumps_left,               # int
                    f"{nana_pref}moonwalkwarning":    nana.moonwalkwarning,          # bool
                    f"{nana_pref}off_stage":          nana.off_stage,                # bool
                    f"{nana_pref}on_ground":          nana.on_ground,                # bool
                    f"{nana_pref}percent":            float(nana.percent),           # float
                    f"{nana_pref}pos_x":              float(nana.position.x),         # float
                    f"{nana_pref}pos_y":              float(nana.position.y),         # float
                    f"{nana_pref}shield_strength":    float(nana.shield_strength),    # float
                    f"{nana_pref}speed_air_x_self":   float(nana.speed_air_x_self),   # float
                    f"{nana_pref}speed_ground_x_self": float(nana.speed_ground_x_self),# float
                    f"{nana_pref}speed_x_attack":     float(nana.speed_x_attack),     # float
                    f"{nana_pref}speed_y_attack":     float(nana.speed_y_attack),     # float
                    f"{nana_pref}speed_y_self":       float(nana.speed_y_self),       # float
                    f"{nana_pref}stock":              nana.stock,                    # int
                })

        # ------------------------------------------------ projectiles ----------
        for j in range(MAX_PROJ):
            pp = f"proj{j}_"
            # Initialize placeholders (consistent types)
            row.update({
                f"{pp}frame":   -1,             # int
                f"{pp}owner":   -1,             # int
                f"{pp}pos_x":   float("nan"),   # float
                f"{pp}pos_y":   float("nan"),   # float
                f"{pp}speed_x": float("nan"),   # float
                f"{pp}speed_y": float("nan"),   # float
                f"{pp}subtype": -1,             # int
                f"{pp}type":    "",             # str placeholder
            })
            # Overwrite with real projectile values if present
            if j < len(gs.projectiles):
                proj = gs.projectiles[j]
                row.update({
                    f"{pp}frame":   proj.frame,        # int
                    f"{pp}owner":   proj.owner,        # int
                    f"{pp}pos_x":   float(proj.position.x),   # float
                    f"{pp}pos_y":   float(proj.position.y),   # float
                    f"{pp}speed_x": float(proj.speed.x),      # float
                    f"{pp}speed_y": float(proj.speed.y),      # float
                    f"{pp}subtype": proj.subtype,      # int
                    f"{pp}type":    proj.type.name,    # str
                })

        # ----------------------------------------------------------------------
        # PRE-SEED ALL NANA COLUMNS → identical schema even if ICs never appear
        # ----------------------------------------------------------------------
        NANA_BUTTONS = [
            "BUTTON_A", "BUTTON_B", "BUTTON_X", "BUTTON_Y", "BUTTON_Z",
            "BUTTON_L", "BUTTON_R", "BUTTON_START",
            "BUTTON_D_UP", "BUTTON_D_DOWN", "BUTTON_D_LEFT", "BUTTON_D_RIGHT",
        ]
        NANA_BOOL_FLAGS = [
            "facing", "invulnerable", "moonwalkwarning", "off_stage", "on_ground",
        ]
        # Separate by datatype
        NANA_STRINGS = ["character", "action"]
        NANA_INTS = [
            "action_frame", "hitlag_left", "hitstun_left",
            "invuln_left", "jumps_left", "stock",
        ]
        NANA_FLOATS = [
            "main_x", "main_y", "c_x", "c_y",
            "ecb_bottom_x", "ecb_bottom_y",
            "ecb_left_x",   "ecb_left_y",
            "ecb_right_x",  "ecb_right_y",
            "ecb_top_x",    "ecb_top_y",
            "percent", "pos_x", "pos_y", "shield_strength",
            "speed_air_x_self", "speed_ground_x_self",
            "speed_x_attack", "speed_y_attack", "speed_y_self",
        ]

        for pref in ("p1_nana_", "p2_nana_"):
            for btn in NANA_BUTTONS:
                row.setdefault(f"{pref}btn_{btn}", False)   # bool default
            for flag in NANA_BOOL_FLAGS:
                row.setdefault(f"{pref}{flag}", False)      # bool default
            for field in NANA_STRINGS:
                row.setdefault(f"{pref}{field}", "")       # str default
            for field in NANA_INTS:
                row.setdefault(f"{pref}{field}", -1)       # int default
            for field in NANA_FLOATS:
                row.setdefault(f"{pref}{field}", float("nan"))  # float default

        # ──────────────────────────────────────
        # Encode all categorical fields → ints
        # ──────────────────────────────────────
        row = encode_row(row)
        rows.append(row)

    # =========================================================================
    df_combined = pd.DataFrame(rows)

    # --------------------------------------------------------------------------
    # Cast every float64 column, including all ECBs, to float32
    float64_cols = df_combined.select_dtypes(include=["float64"]).columns
    for col in float64_cols:
        df_combined[col] = df_combined[col].astype("float32")

    # -------- perspectives -----------------------------------------------------
    def perspective(df: pd.DataFrame, self_pref: str, opp_pref: str) -> pd.DataFrame:
        rename = {}
        for col in df.columns:
            if col.startswith(self_pref):
                rename[col] = "self_" + col[len(self_pref):]
            elif col.startswith(opp_pref):
                rename[col] = "opp_" + col[len(opp_pref):]
        return df.rename(columns=rename)

    df_p1 = perspective(df_combined, "p1_", "p2_")
    df_p2 = perspective(df_combined, "p2_", "p1_")

    # -------- filenames --------------------------------------------------------
    stage_slug = slug(first_stage)
    p1_slug    = slug(first_p1_char)
    p2_slug    = slug(first_p2_char)
    ts_slug    = slug(timestamp)
    uniq       = uuid.uuid4().hex[:8]
    base       = f"{stage_slug}_{p1_slug}_vs_{p2_slug}_{ts_slug}_{uniq}"

    # -------- write ------------------------------------------------------------
    df_p1.to_parquet(OUT_DIR / f"{base}-p1.parquet", index=False)
    df_p2.to_parquet(OUT_DIR / f"{base}-p2-parquet", index=False)

    print(
        f"Wrote {len(df_combined)} frames × {df_combined.shape[1]} columns "
        f"→ {base}{{-p1,-p2}}.parquet"
    )
