# slippi-frame-extractor

A fixed schema pipeline that converts Slippi replays into per frame Parquet datasets with separate **P1** and **P2** views for next input prediction models.

## Features

| Capability | Detail |
|------------|--------|
| **Frame-by-frame extraction** | Parses `.slp` replays with the `melee` bindings. |
| **Guaranteed 250-column schema** | `MAX_PROJ = 8` projectile slots plus pre-seeded Nana fields ensure every replay has identical columns. |
| **Two player-centric outputs** | `*-p1` (Port 1 ⇒ `self_…`, Port 2 ⇒ `opp_…`) and `*-p2` (mirror). |
| **Dtype normalization** | Nana buttons & flags → **bool**; projectile `type` columns → **string** with sentinel `"NONE"`. |
| **Self-describing filenames** | `<stage>_<p1char>_vs_<p2char>_<timestamp>_<uid>-p{1,2}.parquet` |

## Column Breakdown (250 total)

| Group | Columns |
|-------|:------:|
| Frame metadata | 4 |
| Player scalar/control (×2) | 46 × 2 |
| Nana pre-seeded (×2) | 87 × 2 |
| Projectiles (`8 slots × 8`) | 64 |
| **Total** | **250** |

## Quick Start

1. Edit `SLP_PATH` inside **extract.py**.  
2. Run:  
   `python3 extract.py`  
   → files like  
   `<stage>_<p1>_vs_<p2>_<timestamp>_<uid>-p1.parquet`  
   `<stage>_<p1>_vs_<p2>_<timestamp>_<uid>-p2.parquet`

```python
import pandas as pd
df = pd.read_parquet("…-p1.parquet")
tensor = embed_frame(df.iloc[0])  # your embedder
```

## Dependencies

```bash
pip install pandas pyarrow python-slippi melee
```

## License

MIT for the script; Slippi replay files remain yours.

