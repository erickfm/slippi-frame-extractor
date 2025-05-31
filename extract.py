#!/usr/bin/env python3
"""
Dump a Slippi replay to two parquet files with a **fixed column
schema across all replays**.

Files produced ( <base> = <stage>_<p1>_vs_<p2>_<timestamp>_<uuid> ):

  • <base>-p1.{parquet}         – p1_ → self_, p2_ → opp_
  • <base>-p2.{parquet}         – p2_ → self_, p1_ → opp_
"""

import re
import uuid
import unicodedata
import pathlib
import pandas as pd
from melee import Console
from melee.enums import Menu

# --------------------------------------------------------------------------
SLP_PATH = "./"
MAX_PROJ   = 8                    # hard cap on projectile slots
PROJ_FIELDS = (
    "frame", "owner",
    "pos_x", "pos_y",
    "speed_x", "speed_y",
    "subtype", "type",
)

# --------------------------------------------------------------------------
def slug(s: str | None) -> str:
    """Lower-case, de-accent, replace spaces/unsafe chars with '_'."""
    if not s:
        return "unknown"
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    s = re.sub(r"[^\w\s-]", "", s.lower()).strip()
    return re.sub(r"[-\s]+", "_", s)

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

    # ------------------------------------------------ frame-level ----------
    if first_stage is None:
        first_stage = gs.stage.name if gs.stage else "unknown_stage"
        timestamp   = gs.startAt                          # e.g. '2023-05-02T03:39:34Z'

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

        row[f"{pref}port"]          = port
        row[f"{pref}character"]     = ps.character.name
        row[f"{pref}action"]        = ps.action.name
        row[f"{pref}action_frame"]  = ps.action_frame

        # buttons
        for btn, state in ps.controller_state.button.items():
            row[f"{pref}btn_{btn.name}"] = state

        # sticks & shoulders
        row[f"{pref}main_x"], row[f"{pref}main_y"] = ps.controller_state.main_stick
        row[f"{pref}c_x"],    row[f"{pref}c_y"]    = ps.controller_state.c_stick
        row[f"{pref}l_shldr"] = ps.controller_state.l_shoulder
        row[f"{pref}r_shldr"] = ps.controller_state.r_shoulder

        # misc scalars
        row.update({
            f"{pref}costume":              ps.costume,
            f"{pref}ecb_bottom":           ps.ecb_bottom,
            f"{pref}ecb_left":             ps.ecb_left,
            f"{pref}ecb_right":            ps.ecb_right,
            f"{pref}ecb_top":              ps.ecb_top,
            f"{pref}facing":               ps.facing,
            f"{pref}hitlag_left":          ps.hitlag_left,
            f"{pref}hitstun_left":         ps.hitstun_frames_left,
            f"{pref}invuln_left":          ps.invulnerability_left,
            f"{pref}invulnerable":         ps.invulnerable,
            f"{pref}jumps_left":           ps.jumps_left,
            f"{pref}moonwalkwarning":      ps.moonwalkwarning,
            f"{pref}off_stage":            ps.off_stage,
            f"{pref}on_ground":            ps.on_ground,
            f"{pref}percent":              ps.percent,
            f"{pref}pos_x":                ps.position.x,
            f"{pref}pos_y":                ps.position.y,
            f"{pref}shield_strength":      ps.shield_strength,
            f"{pref}speed_air_x_self":     ps.speed_air_x_self,
            f"{pref}speed_ground_x_self":  ps.speed_ground_x_self,
            f"{pref}speed_x_attack":       ps.speed_x_attack,
            f"{pref}speed_y_attack":       ps.speed_y_attack,
            f"{pref}speed_y_self":         ps.speed_y_self,
            f"{pref}stock":                ps.stock,
        })

        # ------------------------------------------------ Nana -------------
        nana = ps.nana
        nana_pref = f"{pref}nana_"
        if nana:
            row.update({
                f"{nana_pref}character":    nana.character.name,
                f"{nana_pref}action":       nana.action.name,
                f"{nana_pref}action_frame": nana.action_frame,
            })
            for btn, state in nana.controller_state.button.items():
                row[f"{nana_pref}btn_{btn.name}"] = state
            row[f"{nana_pref}main_x"], row[f"{nana_pref}main_y"] = nana.controller_state.main_stick
            row[f"{nana_pref}c_x"],    row[f"{nana_pref}c_y"]    = nana.controller_state.c_stick
            row[f"{nana_pref}l_shldr"] = nana.controller_state.l_shoulder
            row[f"{nana_pref}r_shldr"] = nana.controller_state.r_shoulder
            row.update({
                f"{nana_pref}costume":              nana.costume,
                f"{nana_pref}ecb_bottom":           nana.ecb_bottom,
                f"{nana_pref}ecb_left":             nana.ecb_left,
                f"{nana_pref}ecb_right":            nana.ecb_right,
                f"{nana_pref}ecb_top":              nana.ecb_top,
                f"{nana_pref}facing":               nana.facing,
                f"{nana_pref}hitlag_left":          nana.hitlag_left,
                f"{nana_pref}hitstun_left":         nana.hitstun_frames_left,
                f"{nana_pref}invuln_left":          nana.invulnerability_left,
                f"{nana_pref}invulnerable":         nana.invulnerable,
                f"{nana_pref}jumps_left":           nana.jumps_left,
                f"{nana_pref}moonwalkwarning":      nana.moonwalkwarning,
                f"{nana_pref}off_stage":            nana.off_stage,
                f"{nana_pref}on_ground":            nana.on_ground,
                f"{nana_pref}percent":              nana.percent,
                f"{nana_pref}pos_x":                nana.position.x,
                f"{nana_pref}pos_y":                nana.position.y,
                f"{nana_pref}shield_strength":      nana.shield_strength,
                f"{nana_pref}speed_air_x_self":     nana.speed_air_x_self,
                f"{nana_pref}speed_ground_x_self":  nana.speed_ground_x_self,
                f"{nana_pref}speed_x_attack":       nana.speed_x_attack,
                f"{nana_pref}speed_y_attack":       nana.speed_y_attack,
                f"{nana_pref}speed_y_self":         nana.speed_y_self,
                f"{nana_pref}stock":                nana.stock,
            })

    # ------------------------------------------------ projectiles ----------
    for j in range(MAX_PROJ):
        pp = f"proj{j}_"
        for f in PROJ_FIELDS:               # seed
            row[f"{pp}{f}"] = None
        if j < len(gs.projectiles):         # overwrite if exists
            proj = gs.projectiles[j]
            row.update({
                f"{pp}frame":   proj.frame,
                f"{pp}owner":   proj.owner,
                f"{pp}pos_x":   proj.position.x,
                f"{pp}pos_y":   proj.position.y,
                f"{pp}speed_x": proj.speed.x,
                f"{pp}speed_y": proj.speed.y,
                f"{pp}subtype": proj.subtype,
                f"{pp}type":    proj.type.name,
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
    NANA_SCALARS = [
        "character", "action", "action_frame",
        "main_x", "main_y", "c_x", "c_y",
        "l_shldr", "r_shldr",
        "costume", "ecb_bottom", "ecb_left", "ecb_right", "ecb_top",
        "hitlag_left", "hitstun_left", "invuln_left", "jumps_left",
        "percent", "pos_x", "pos_y", "shield_strength",
        "speed_air_x_self", "speed_ground_x_self",
        "speed_x_attack", "speed_y_attack", "speed_y_self",
        "stock",
    ]

    for pref in ("p1_nana_", "p2_nana_"):
        for btn in NANA_BUTTONS:
            row.setdefault(f"{pref}btn_{btn}", False)    # bool default
        for flag in NANA_BOOL_FLAGS:
            row.setdefault(f"{pref}{flag}", False)       # bool default
        for field in NANA_SCALARS:
            row.setdefault(f"{pref}{field}", None)       # numeric / str default

    rows.append(row)

# ==========================================================================
df_combined = pd.DataFrame(rows)

# -------- dtype fixes ------------------------------------------------------
nana_bool_cols = [
    c for c in df_combined.columns
    if c.startswith(("p1_nana_", "p2_nana_")) and
       (c.startswith(("p1_nana_btn_", "p2_nana_btn_")) or
        c.endswith(("_facing", "_invulnerable",
                    "_moonwalkwarning", "_off_stage", "_on_ground")))
]
df_combined[nana_bool_cols] = df_combined[nana_bool_cols].astype(bool)

proj_type_cols = [c for c in df_combined.columns if re.fullmatch(r"proj\d+_type", c)]
df_combined[proj_type_cols] = df_combined[proj_type_cols].fillna("NONE").astype("string")

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
outdir     = pathlib.Path(".")

# -------- write ------------------------------------------------------------
df_p1.to_parquet       (outdir / f"{base}-p1.parquet",       index=False)
df_p2.to_parquet       (outdir / f"{base}-p2.parquet",       index=False)

print(
    f"Wrote {len(df_combined)} frames × {df_combined.shape[1]} columns "
    f"→ {base}-{{p1,p2}}.parquet"
)
