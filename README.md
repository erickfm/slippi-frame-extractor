# slippi-frame-extractor

A fixed‐schema pipeline that converts Slippi replays into per‐frame Parquet datasets, with separate P1/P2 views for next‐input‐prediction models.


## Features

- **Frame‐by‐frame extraction**  
  Parses `.slp` replays using the Melee bindings.

- **Guaranteed 260-column schema**  
  - `MAX_PROJ = 8` projectile slots  
  - Pre-seeded Nana fields  
  Ensures every replay produces identical columns, even if no projectile or Nana data is present.

- **Two player-centric outputs**  
  - `*-p1.parquet`: Port 1 → `self_*…`, Port 2 → `opp_*…`  
  - `*-p2.parquet`: Port 2 → `self_*…`, Port 1 → `opp_*…` (mirrored)

- **Dtype normalization**  
  - Nana buttons & flags → `bool`  
  - Projectile `type` columns → `string` (with sentinel value `""` when empty)

- **Self-describing filenames**  
  Format:  
```
<stage><p1char>vs<p2char><YYYYMMDD-HHMMSS>_<uuid>-p{1,2}.parquet
```
Example:  
```
FinalDestination_Fox_vs_Marth_20250601-153045_abcd1234-p1.parquet
```


## Column Breakdown (260 total)

| Group                           | Columns               |
|---------------------------------|-----------------------|
| **Frame metadata**              | 4                     |
| **Player scalar/control** × 2   | 50 × 2 = 100          |
| **Projectiles (8 slots × 8)**   | 64                    |
| **Nana pre-seeded** × 2          | 46 × 2 = 92           |
| **Total**                       | **260**               |

- “Player scalar/control” refers to things like button flags, positions, percent, stock, etc.  
- “Nana pre-seeded” fields correspond to the Ice Climbers partner data, expanded so they’re always present (even if unused).


## Quick Start

1. **Edit `SLP_PATH` inside `extract.py`.**  
 Point it to your directory of `.slp` files, for example:  
 ```python
 SLP_PATH = "/Volumes/slippi/ranked-anonymized/"
```
2. Run the extractor:
```bash
python3 extract.py
```
3. Result:
Two Parquet files for each .slp replay, for example:
```
FinalDestination_Fox_vs_Marth_20250601-153045_abcd1234-p1.parquet
FinalDestination_Fox_vs_Marth_20250601-153045_abcd1234-p2.parquet
```
4. Load and embed:
```python
import pandas as pd

df = pd.read_parquet("…-p1.parquet")
tensor = embed_frame(df.iloc[0])  # your custom embedding function
```


## Dependencies

```bash
pip install pandas pyarrow python-slippi melee
```
pandas: for DataFrame manipulation
pyarrow: Parquet I/O
python-slippi: parsers and data structures for Slippi .slp files
melee: Melee-specific enums and game-state bindings


## License

The script is released under the MIT License.
Slippi replay files remain the user’s property.

