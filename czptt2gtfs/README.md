# CZPTT@GTFS

Converter utility for converting Czech train XML data (CZPTT, published by Správa železnic) to standard GTFS format.
Based mostly on the [KSP MFF](https://ksp.mff.cuni.cz/h/ulohy/32/serial-jr/) competition. All credits to the original authors.

## Usage in this project

The converter is normally run by the official GTFS pipeline, which downloads the base archive and all monthly updates, merges the XML and converts it:

```bash
./venv/bin/python scripts/download_and_convert_official_gtfs.py --year 2026 --work-dir data/official_rail_work
```

See the main [README](../README.md#download-and-convert-official-rail-gtfs) for details.

## Standalone usage

1. Get the data from https://portal.cisjr.cz/pub/draha/celostatni/szdc/YYYY/ (`JRyyyy.zip` plus monthly update folders) and extract the XML files into one directory.

2. Run the converter:

```bash
python -m czptt2gtfs <input_xml_dir> <output_gtfs_dir>
```

Or after installation (`pip install -e .` in the repo root):

```bash
czptt2gtfs <input_xml_dir> <output_gtfs_dir>
```

Options:

- `--sr70`: CSV with SR70 station codes (default `czptt2gtfs/data/sr70.csv`).
- `--komercni-druhy`: XML with commercial train types (default `czptt2gtfs/data/komercni_druhy.xml`).

## Reference data

- `data/sr70.csv`: station list (SR70 code, name, coordinates); used for GTFS `stops.txt`.
- `data/komercni_druhy.xml`: commercial train types (Os, Sp, R, …). Refresh it from the Správa železnic KADR web service:

```bash
cd czptt2gtfs/data
./komercni_druhy.sh > komercni_druhy.xml
```

## Generating timetables with gtfs-to-html (optional)

```bash
gtfs-to-html --configPath config.gtfs-to-html.json
```

## References

- https://dadof.ggu.cz/d/3-zdroje-dat-o-ve-ejn-doprav/
- https://ksp.mff.cuni.cz
- https://gtfs-validator.mobilitydata.org/
- https://gtfstohtml.com/docs/
- [more sources with mobility data](https://github.com/MobilityData/awesome-transit?tab=readme-ov-file)
