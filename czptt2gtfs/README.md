# CZPTT@GTFS

Converter utility for converting between Czech train XML data to standard GTFS format.
Based mostly on the [KSP MFF](https://ksp.mff.cuni.cz/h/ulohy/32/serial-jr/) competition. All credits to the original authors.

## Usage

1. Get the data from ftp://ftp.cisjr.cz/draha/celostatni/szdc/YYYY/JRyyyy.zip and extract the XML files.

2. Update the train types

```bash
./komercni_druhy.sh > komercni_druhy.xml 
```

3. Run the converter utility:

```
python -m czptt2gtfs <input_xml_dir> <output_gtfs_dir>
```

Or after installation:

```bash
czptt2gtfs <input_xml_dir> <output_gtfs_dir>
```

4. generating timetables

```bash
gtfs-to-html --configPath config.gtfs-to-html.json
```

## References

- https://dadof.ggu.cz/d/3-zdroje-dat-o-ve-ejn-doprav/
- https://ksp.mff.cuni.cz
- https://gtfs-validator.mobilitydata.org/
- https://gtfstohtml.com/docs/
- [more sources with mobility data](https://github.com/MobilityData/awesome-transit?tab=readme-ov-file)
