# Python scripts used to scrape [Harvard Personal Genome Project](https://pgp.med.harvard.edu) site
- **scrape-pgp-harvard.py:** checks all participants on specified genome sequence provider's page and writes a summary .txt file containing patient id's with a viable genome download link and indications of whether an EHR or survey entry is available on their page
- **hvd-pgp-retrieve-ehrs-surveys:** Using output from previous script, accesses each participant's page and saves any EHR and survey information present
