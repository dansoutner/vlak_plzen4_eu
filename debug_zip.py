import gzip
import zipfile
from pathlib import Path

problem_file = Path("/Users/dan/Data/STAN/jizdni_rady/data/official_rail_work/downloads/2026/2025-11/PA_0054_--KADR058625_01_2026.xml.zip")

# Decompress gzip first
with gzip.open(problem_file, 'rb') as gz_file:
    decompressed_data = gz_file.read()

print(len(decompressed_data))
# data sample
print(decompressed_data[:100])

