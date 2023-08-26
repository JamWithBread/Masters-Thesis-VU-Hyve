# Python and bash scripts used to acquire and check integrity of 23andMe and Complete Genomics files attached to identified profiles.
- **download-PGP-genomes.py:** Takes .txt file of identified Harvard PGP profiles and scrapes **specified** genomics provider page (23andMe or Complete Genomics). Locates and downloads correct file.
- **unzip_23andMe.sh & unzip_CG.sh:** Force unzips downloaded files.
- **remove-invalid-23andMe-files.ipynb & check-GC-integrity.py:** Checks integrity of files (valid header, encoding, genome build info, column names, file size, provider). Removes invalid files.
