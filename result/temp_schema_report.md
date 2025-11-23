# JSON Schema Report

Generated: 2025-11-23 18:17:48

---

## File: iris.json

- **Records Scanned:** 150
- **Fields Detected:** 5

### Field Details

| Field Name | Type | Nullable | Example Value | Statistics | Notes |
|------------|------|----------|---------------|------------|-------|
| `petalLength` | unknown | No | `1.4` | - | - |
| `petalWidth` | unknown | No | `0.2` | - | - |
| `sepalLength` | unknown | No | `5.1` | - | - |
| `sepalWidth` | unknown | No | `3.5` | - | - |
| `species` | string | No | `setosa` | len: 6-10 (avg: 8.3); enum: setosa, versicolor, virginica | enum-like |

---

## File: global_gross_production_of_crops_livestock.json

- **Records Scanned:** 50
- **Fields Detected:** 7

### Field Details

| Field Name | Type | Nullable | Example Value | Statistics | Notes |
|------------|------|----------|---------------|------------|-------|
| `product` | string | No | `Rice, paddy` | len: 3-44 (avg: 14.3) | - |
| `production_volume_metric_tons` | string | No | `751,885,117` | len: 9-13 (avg: 10.3) | - |
| `rank` | integer | No | `1` | min: 1, max: 50 | - |
| `top_country_value_usd_billion` | string | No | `$117` | len: 4-5 (avg: 4.9) | - |
| `top_producing_country` | string | No | `Mainland China` | len: 5-14 (avg: 11.8); enum: 10 values | enum-like |
| `total_value_usd_billion` | string | No | `$332` | len: 4-5 (avg: 4.8) | - |
| `value_per_metric_ton_usd` | string | No | `$442` | len: 3-6 (avg: 4.6) | - |

---

## File: stackoverflow_combined_info.json

- **Records Scanned:** 1
- **Fields Detected:** 5

### Field Details

| Field Name | Type | Nullable | Example Value | Statistics | Notes |
|------------|------|----------|---------------|------------|-------|
| `columns` | integer | No | `34` | min: 34, max: 34 | - |
| `description` | string | No | `Main dataset file` | len: 17-17 (avg: 17.0) | - |
| `name` | string | No | `stackoverflow_questions.csv` | len: 27-27 (avg: 27.0) | - |
| `rows` | integer | No | `95636` | min: 95636, max: 95636 | - |
| `totalBytes` | integer | No | `118833842` | min: 118833842, max: 118833842 | - |

---

## File: top_coffee_producing_countries.json

- **Records Scanned:** 20
- **Fields Detected:** 3

### Field Details

| Field Name | Type | Nullable | Example Value | Statistics | Notes |
|------------|------|----------|---------------|------------|-------|
| `country` | string | No | `Brazil` | len: 4-16 (avg: 7.9) | - |
| `major_regions` | string | No | `Minas Gerais, Espírito Santo, São Paulo, Bahia,...` | len: 14-57 (avg: 43.4) | - |
| `percentage_of_world_production` | string | No | `37.4%` | len: 4-5 (avg: 4.1) | - |

---

## File: rows.json

- **Records Scanned:** 10000
- **Fields Detected:** 28

### Field Details

| Field Name | Type | Nullable | Example Value | Statistics | Notes |
|------------|------|----------|---------------|------------|-------|
| `column_0` | string | No | `row-9zmh-kxbg.u7ht` | len: 18-18 (avg: 18.0) | - |
| `column_1` | uuid | No | `00000000-0000-0000-D2B6-825AE6F8F1CE` | len: 36-36 (avg: 36.0) | - |
| `column_10` | string | Yes | `Olympia` | len: 3-24 (avg: 8.2) | nullable |
| `column_11` | string | No | `WA` | len: 2-2 (avg: 2.0); enum: AE, CA, ID, WA, WI | enum-like |
| `column_12` | numeric_string | Yes | `98512` | min: 53588.0, max: 99403.0; len: 5-5 (avg: 5.0) | nullable |
| `column_13` | numeric_string | No | `2024` | min: 2008.0, max: 2026.0; len: 4-4 (avg: 4.0) | enum-like |
| `column_14` | string | No | `AUDI` | len: 3-13 (avg: 5.6) | - |
| `column_15` | mixed(numeric_string, string) | No | `Q5 E` | min: 500.0, max: 500.0; len: 2-21 (avg: 6.6) | mixed types |
| `column_16` | string | No | `Plug-in Hybrid Electric Vehicle (PHEV)` | len: 30-38 (avg: 31.7); enum: Battery Electric Vehicle (BEV), Plug-in Hybrid Electric Vehicle (PHEV) | enum-like |
| `column_17` | string | No | `Not eligible due to low battery range` | len: 37-60 (avg: 51.3); enum: Clean Alternative Fuel Vehicle Eligible, Eligibility unknown as battery range has not been researched, Not eligible due to low battery range | enum-like |
| `column_18` | numeric_string | No | `23` | min: 0.0, max: 337.0; len: 1-3 (avg: 1.6) | - |
| `column_19` | numeric_string | No | `0` | min: 0.0, max: 184400.0; len: 1-6 (avg: 1.0) | - |
| `column_2` | integer | No | `0` | min: 0, max: 0; enum: 0 | enum-like |
| `column_20` | numeric_string | Yes | `22` | min: 1.0, max: 49.0; len: 1-2 (avg: 1.9) | nullable |
| `column_21` | numeric_string | No | `263239938` | min: 135061.0, max: 478926346.0; len: 6-9 (avg: 9.0) | - |
| `column_22` | string | Yes | `POINT (-122.90787 46.9461)` | len: 24-27 (avg: 26.7) | nullable |
| `column_23` | string | Yes | `PUGET SOUND ENERGY INC` | len: 10-112 (avg: 43.8) | nullable |
| `column_24` | numeric_string | Yes | `53067010910` | min: 6019004211.0, max: 55111000800.0; len: 11-11 (avg: 11.0) | nullable |
| `column_25` | numeric_string | Yes | `2742` | min: 848.0, max: 3214.0; len: 3-4 (avg: 4.0) | nullable |
| `column_26` | numeric_string | Yes | `10` | min: 1.0, max: 10.0; len: 1-2 (avg: 1.1); enum: 10 values | nullable, enum-like |
| `column_27` | numeric_string | Yes | `39` | min: 1.0, max: 49.0; len: 1-2 (avg: 1.8) | nullable |
| `column_3` | integer | No | `1760391073` | min: 1760391073, max: 1760391073; enum: 1760391073 | enum-like |
| `column_4` | null | Yes | `None` | - | nullable |
| `column_5` | integer | No | `1760391201` | min: 1760391201, max: 1760391251 | enum-like |
| `column_6` | null | Yes | `None` | - | nullable |
| `column_7` | json_string | No | `{ }` | len: 3-3 (avg: 3.0); enum: { } | enum-like |
| `column_8` | string | No | `WA1E2AFY8R` | len: 10-10 (avg: 10.0) | - |
| `column_9` | string | Yes | `Thurston` | len: 4-12 (avg: 5.3) | nullable |

---
