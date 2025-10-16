# Fly Sleep Behavior Relational Database

This project implements a **relational database design** for fly sleep behavior data to avoid redundancy and enable efficient analysis.

## 🏗️ Database Architecture

### Two-Table Design

**TABLE 1: `time_series_data.csv`** (530,240 rows, 16.5 MB)
- **Purpose**: Stores raw measurements only
- **Columns**: `datetime`, `monitor`, `channel`, `mt`, `ct`, `pn`
- **No redundancy**: Fly metadata stored separately

**TABLE 2: `fly_metadata.csv`** (64 rows, 1.9 KB)
- **Purpose**: Stores fly information once per fly
- **Columns**: `monitor`, `channel`, `fly_id`, `genotype`, `sex`, `treatment`
- **Links to time_series**: Via `(monitor, channel)` foreign key

### Why This Design?

❌ **BAD**: Store everything in one table
```
datetime, monitor, channel, mt, ct, pn, genotype, sex, treatment
2025-09-19 11:46:00, 5, 1, 15, 26, 15, SSS, Female, 2mM His
2025-09-19 11:47:00, 5, 1, 15, 26, 15, SSS, Female, 2mM His  # REDUNDANT!
2025-09-19 11:48:00, 5, 1, 15, 26, 15, SSS, Female, 2mM His  # REDUNDANT!
...530,000 more rows with same genotype/sex/treatment...
```

✅ **GOOD**: Relational design
```
# time_series_data.csv (lean, no redundancy)
datetime, monitor, channel, mt, ct, pn
2025-09-19 11:46:00, 5, 1, 15, 26, 15
2025-09-19 11:47:00, 5, 1, 15, 26, 15
2025-09-19 11:48:00, 5, 1, 15, 26, 15

# fly_metadata.csv (stored once)
monitor, channel, fly_id, genotype, sex, treatment
5, 1, M5_Ch01, SSS, Female, 2mM His
```

## 📊 Dataset Overview

- **64 flies** across 2 monitors (5 & 6)
- **4 genotypes**: SSS, Rye, Fmn, Iso
- **3 treatments**: 2mM His, 8mM His, VEH
- **530,240 measurements** over 6 days
- **Date range**: 2025-09-19 to 2025-09-25

## 🚀 Quick Start

### 1. Create the Database
```bash
cd src
python3 create_database.py
```

### 2. Use in Analysis
```python
import pandas as pd

# Load the two tables
time_series = pd.read_csv('data/processed/time_series_data.csv')
metadata = pd.read_csv('data/processed/fly_metadata.csv')

# Join them for complete analysis
full_data = time_series.merge(metadata, on=['monitor', 'channel'])

# Now you have everything: datetime, monitor, channel, mt, ct, pn, fly_id, genotype, sex, treatment
print(full_data.head())
```

### 3. Run Demonstration
```bash
cd src
python3 demo_relational_database.py
```

## 📁 File Structure

```
fly-ML/
├── src/
│   ├── create_database.py          # Creates the relational database
│   └── demo_relational_database.py # Demonstrates usage
├── data/
│   └── processed/
│       ├── fly_metadata.csv        # TABLE 2: Fly information (64 rows)
│       └── time_series_data.csv    # TABLE 1: Measurements (530K rows)
├── results/
│   └── figures/                    # Generated analysis plots
├── details.txt                     # Source metadata
├── Monitor5.txt                    # Source time-series data
├── Monitor6.txt                    # Source time-series data
└── README_relational_database.md   # This file
```

## 🔍 Data Schema

### time_series_data.csv
| Column | Type | Description |
|--------|------|-------------|
| `datetime` | datetime | When measurement was taken |
| `monitor` | int | Monitor number (5 or 6) |
| `channel` | int | Channel number (1-32) |
| `mt` | int | Movement count (active beam breaks) |
| `ct` | int | Cumulative total |
| `pn` | int | Pause/inactive periods |

### fly_metadata.csv
| Column | Type | Description |
|--------|------|-------------|
| `monitor` | int | Monitor number (5 or 6) |
| `channel` | int | Channel number (1-32) |
| `fly_id` | str | Unique fly identifier (M5_Ch01, M6_Ch15, etc.) |
| `genotype` | str | Fly genotype (SSS, Rye, Fmn, Iso) |
| `sex` | str | Fly sex (Female) |
| `treatment` | str | Treatment condition (2mM His, 8mM His, VEH) |

## 💡 Benefits of Relational Design

### 1. **No Redundancy**
- Genotype stored once per fly, not 530K times
- File sizes stay manageable
- Easy to update fly information

### 2. **Efficient Storage**
- Time-series: 16.5 MB (vs ~50 MB with redundancy)
- Metadata: 1.9 KB
- Total: 16.5 MB (vs ~50 MB with redundancy)

### 3. **Easy Updates**
- Change fly info in one place (metadata table)
- No need to update millions of time-series rows
- Add new metadata columns without touching time-series

### 4. **Professional Design**
- Follows database normalization principles
- Scalable to larger datasets
- Industry-standard approach

## 📈 Analysis Examples

### Basic Analysis
```python
# Load and join data
time_series = pd.read_csv('data/processed/time_series_data.csv')
metadata = pd.read_csv('data/processed/fly_metadata.csv')
full_data = time_series.merge(metadata, on=['monitor', 'channel'])

# Analyze by genotype
genotype_stats = full_data.groupby('genotype')['mt'].agg(['mean', 'std', 'count'])
print(genotype_stats)

# Analyze by treatment
treatment_stats = full_data.groupby('treatment')['mt'].agg(['mean', 'std', 'count'])
print(treatment_stats)
```

### Time Series Analysis
```python
# Get data for one specific fly
fly_data = full_data[full_data['fly_id'] == 'M5_Ch01'].copy()
fly_data = fly_data.sort_values('datetime')

# Plot movement over time
import matplotlib.pyplot as plt
plt.plot(fly_data['datetime'], fly_data['mt'])
plt.title(f'Movement over time - {fly_data.iloc[0]["fly_id"]} ({fly_data.iloc[0]["genotype"]})')
plt.show()
```

### Comparative Analysis
```python
# Compare genotypes
sss_data = full_data[full_data['genotype'] == 'SSS']
iso_data = full_data[full_data['genotype'] == 'Iso']

print(f"SSS average movement: {sss_data['mt'].mean():.2f}")
print(f"Iso average movement: {iso_data['mt'].mean():.2f}")
```

## 🛠️ Scripts

### `create_database.py`
- Parses `details.txt` → creates `fly_metadata.csv`
- Parses `Monitor5.txt` and `Monitor6.txt` → creates `time_series_data.csv`
- Demonstrates the relational JOIN
- Shows summary statistics

### `demo_relational_database.py`
- Loads both tables
- Demonstrates JOIN operation
- Performs sample analyses
- Creates visualizations

## 📊 Performance

- **Database creation**: ~30 seconds
- **File sizes**: 16.5 MB total (vs ~50 MB with redundancy)
- **Memory usage**: Efficient for analysis
- **Query speed**: Fast JOIN operations

## 🔧 Requirements

```bash
pip install pandas numpy matplotlib seaborn
```

## 📝 Notes

- All flies are Female in this dataset
- Empty channels (31, 32) are excluded from metadata
- Time-series data includes only active channels (non-zero measurements)
- Datetime parsing handles the specific format in Monitor files
- The design scales to larger datasets and more monitors

This relational database design provides a professional, efficient, and scalable foundation for fly sleep behavior analysis.
