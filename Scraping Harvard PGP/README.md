# Python scripts used to scrape [Harvard Personal Genome Project](https://pgp.med.harvard.edu) site
- **scrape-pgp-harvard.py:** Checks all participants on specified genome sequence provider's page. For participants with a viable genome download file, writes a summary .txt file containing patient ID and indication of whether an EHR and survey entry is available on their page.
- **hvd-pgp-retrieve-ehrs-surveys:** Using output from previous script, accesses each participant's page and saves any EHR and survey information present.
