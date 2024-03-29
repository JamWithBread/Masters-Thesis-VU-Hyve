# Master's Thesis
<span style="display: inline-block;">
  <img src="https://github.com/JamWithBread/Masters-Thesis-VU-Hyve/blob/main/Images%3AIllustrations/Logos/VU_Logo.png" width="200" alt="VU Logo" />
</span>
<span style="display: inline-block;">
  <img src="https://github.com/JamWithBread/Masters-Thesis-VU-Hyve/blob/main/Images%3AIllustrations/Logos/Hyve_Logo.png" width="200" alt="Hyve Logo" />
</span>

### _OMOP CDM-Enabled Genomics Exploration: Unveiling Population Structure and PRS Associations from the Harvard Personal Genome Project_
Jeren Olsen
#
## About the Repository
This repository contains all code developed for the completion of my master's thesis project done at [The Hyve](https://www.thehyve.nl).
## Project Contents
- [Abstract](#abstract)
- [Acquiring Harvard Personal Genome Project Dataset - Scraping](#acquiring-harvard-personal-genome-project-dataset---scraping)
- [Transformation Pipelines](#transformation-pipelines)
  - [Observational Health Data](#observational-health-data)
  - [Genomics Data](#genomics-data)
- [Analysis Methods](#analysis-methods)
- [Summary of Results](#summary-of-results)
- [Software Used](#software-used)
- [Data Availability](#data-availability)

<a name="abstract"></a>
## Abstract
With the increasing availability of detailed and diverse data types associated with patient profiles in health data repositories, there is a great opportunity to combine these data to allow for more comprehensive representations of patient profiles in large scale health research. In particular, the ability to include genomics data in federated analyses utilizing the Observational Medical Outcomes Partnership Common Data Model (OMOP CDM) would be of great benefit. Therefore, this thesis project sought to identify modern genomics tools that could integrate well with the OMOP CDM for research studies leveraging both observational and genomic information. The Google Cloud Life Sciences API was identified to be fit for purpose due to its scalability, availability of an intuitive and open-source variant representation tool, integrated compute environment and excellent documentation. To demonstrate the utility of combining the OMOP CDM with GCP Life Sciences API, population structure analyses and a polygenic risk score (PRS) study were carried out on a study population derived from the Harvard Personal Genome Project. Separate ETL pipelines were applied to the electronic health record data and genomics data from the identified study population. An analysis methodology was developed leveraging the ATLAS cohort building tool alongside the transformed health data in the OMOP CDM and genomics data in Google’s BigQuery variant schema. Chronic disease prevalence was analyzed alongside occurrences of clinically significant variants with potential phenotype associations. Lastly, a cohort based PRS analysis was carried out to explore associations between polygenic profiles and occurrences of the Gastroesophageal Reflux Disease (GERD) phenotype. While no clinically significant variants with phenotype associations were found, a significant association between PRS and the GERD phenotype was discovered. In light of the growing interest in leveraging EHR data for comprehensive health research, this study contributes to the ongoing movement towards the integration of genetic and observational health data.

<a name="acquiring-harvard-personal-genome-project-dataset---scraping"></a>
## Acquiring Harvard Personal Genome Project Dataset - Scraping
<img src="https://github.com/JamWithBread/Masters-Thesis-VU-Hyve/blob/main/Images%3AIllustrations/Logos/Harvard%20PGP.png" width="350" alt="VU Logo" /><br>

For this project, an open-source dataset was curated from the [Harvard Personal Genome Project Site](https://pgp.med.harvard.edu). It consisted of 798 unique individuals (profiles) who possessed publicly available electronic health record data and a DTC-genomics file generated by either 23andMe or Complete Genomics. The profiles and corresponding data were identified and extracted in the multistep scraping process detailed here: [Scraping Harvard PGP](https://github.com/JamWithBread/Masters-Thesis-VU-Hyve/tree/main/Scraping%20Harvard%20PGP)

<a name="transformation-pipelines"></a>
## Transformation Pipelines
To prepare the genomics and observational health data for analysis, respective transformation pipelines were applied.

<a name="observational-health-data"></a>
### Observational Health Data
The observational health data underwent transformation via the source data to OMOP CDM ETL process; a pipeline involving a suite of open-source tools for generating an ETL outline, mapping source vocabulary to standard vocabularies, applying the transformations and targeting a standardized database schema.

#### OHDSI ETL Tools
![OHDSI Tools](https://github.com/JamWithBread/Masters-Thesis-VU-Hyve/blob/main/Images%3AIllustrations/Schematics/OHDSI%20Tools.png)
Tool | Description
--- | ---
**White Rabbit** | Scan report of source data. Provides summary statistics and helps understand source data in context of OMOP CDM.
**Rabbit in a Hat** | Creates visual mapping of source to target tables. Aids in transformation writing process.
**Usagi** | Generates semantic source vocab to target vocab mappings using Athena CDM 5.4 standard vocabularies.
**Delphyne** | Python program to write specific source table to target transformations using outputs from previous tools. Targets PostgreSQL database: The OMOP CDM.

<a name="genomics-data"></a>
### Genomics Data
The genomics dataset acquired from the Harvard PGP site consisted of 202 assembly variations files sequenced by Complete Genomics and 595 genotyping files produced by 23andMe. To make these files comparable, a pipeline process was developed to convert the files to the same file format and genome build, perform genome imputation to place the files on a common set of markers, and apply data filtering where appropriate. Quality checks, such as concordance measures, were also made to give an indication of reliability of the chosen processing strategy. A distributed workload approach was used to execute the pipeline outlined below, using Google Cloud's Compute environment. 

See [Genomics_Pipeline_GH_gcp](https://github.com/JamWithBread/Masters-Thesis-VU-Hyve/tree/main/Genomics_Pipeline_GH_gcp) for implementation.

**Genomics Pipeline Workflow**

<img src="https://github.com/JamWithBread/Masters-Thesis-VU-Hyve/blob/main/Images%3AIllustrations/Schematics/Genomics%20Pipeline.png"  width="600" /><br><br>

**Variant Transforms Tool**

<img src="https://github.com/JamWithBread/Masters-Thesis-VU-Hyve/blob/main/Images%3AIllustrations/Logos/gcp-variant-transforms.png" width="250" /> 

| [Repo](https://github.com/googlegenomics/gcp-variant-transforms) | [More Info](https://cloud.google.com/life-sciences/docs/how-tos/variant-transforms) |

Google's Variant Transforms Tool was selected as most fit for purpose for the representation and querying of variant data for this project, and further as a tool that is generally well suited for research at the intersection of observational health outcomes and genomics when combined with the OMOP CDM. It was selected as it fulfilled the following traits:
- Patient-centric
- Open-source
- Implementation accessible to novice researchers
- Scalable 
- Well documented and maintained
- Not restrictive in scope of genomics data

See [Google Variant Transform Tool](https://github.com/JamWithBread/Masters-Thesis-VU-Hyve/tree/main/Google%20Variant%20Transform%20Tool) for implementation.

Note that the tool was used under the Google Life Sciences API, which has been scheduled for deprecation. The tool has migrated to Google Batch service.


<a name="analysis-methods"></a>
## Analysis Methods
- See section **2. Methods** of [Thesis](https://github.com/JamWithBread/Masters-Thesis-VU-Hyve/blob/main/BSB_Masters_Thesis_JerenOlsen.pdf)

<a name="summary-of-results"></a>
## Summary of Results
- See section **3. Results** of [Thesis](https://github.com/JamWithBread/Masters-Thesis-VU-Hyve/blob/main/BSB_Masters_Thesis_JerenOlsen.pdf)

<a name="software-used"></a>
## Software Used
#### Genomics
|Name|Version|Reference|
| --- | --- | --- |
| Google Cloud SDK | 437.0.1 | https://cloud.google.com/sdk |
| Google Cloud Life Sciences API | v2beta | https://cloud.google.com/life-sciences/docs/reference/rest |
| Variant Transform Tool | 0.11.0 | https://github.com/googlegenomics/gcp-variant-transforms |
| Google Cloud Platform (Bucket, Compute Engine, etc) | As of July 21 release | https://cloud.google.com/release-notes |
| PRSice2 | 2.2.3 | https://choishingwan.github.io/PRS-Tutorial/prsice/ |
| BCFTools | 1.18 | https://www.htslib.org/download/ |
| SAMTools | 1.18 | https://www.htslib.org/download/ |
| Beagle 5.4 | 5.4 | http://faculty.washington.edu/browning/beagle/beagle.html |
| GATK Picard | 4.0.2.0 | https://gatk.broadinstitute.org/hc/en-us/articles/360037060932-LiftoverVcf-Picard- |
| Plink | 1.90p | https://plink.readthedocs.io/en/latest/ |
| Java | openjdk-17 | --- |

#### OHDSI 
|Name|Version|Reference|
| --- | --- | --- |
| White Rabbit | 0.10.9 | https://github.com/OHDSI/WhiteRabbit |
| RabbitInAHat | 0.10.8 | https://ohdsi.github.io/WhiteRabbit/RabbitInAHat.html |
| Usagi | 1.4.3 | https://github.com/OHDSI/Usagi |
| Delphyne | 0.2.0 | https://thehyve.nl |
| Apache Tomcat | 8.5.90 | https://tomcat.apache.org |
| Apache Maven | 3.9.3 | https://maven.apache.org |
| ATLAS | 2.13.0 | https://github.com/OHDSI/Atlas |
| PostgreSQL | 14.8 | https://www.postgresql.org |
| Java | 1.8 | --- |

#### General
|Name|Version|Reference|
| --- | --- | --- |
| Python | 3.11 | --- |
| Pandas| 2.0 | --- |

<a name="data-availability"></a>
## Data Availability

- All 798 Harvard PGP participant study IDs can be found [here](https://github.com/JamWithBread/Masters-Thesis-VU-Hyve/blob/main/Resources/Harvard-PGP-798-profiles.txt).

- ClinVar Variants Summary download can be found [here](https://ftp.ncbi.nlm.nih.gov/pub/clinvar/tab_delimited/variant_summary.txt.gz).

- NHANES Chronic diseases dataset can be found here:
  - 2017-2018: [NHANES Dataset](https://wwwn.cdc.gov/Nchs/Nhanes/continuousnhanes/default.aspx?BeginYear=2017).

- Reference Assemblies Used:
  - GRCh37: [GRCh37 Assembly](http://grch37.ensembl.org/Homo_sapiens/Info/Index).
  - GRCh36: [GRCh36 Assembly](https://www.ncbi.nlm.nih.gov/datasets/genome/GCF_000001405.12/).
  - Chain file: [NCBI36_to_GRCh37.chain](https://github.com/JamWithBread/Masters-Thesis-VU-Hyve/blob/main/Resources/NCBI36_to_GRCh37.chain).

- 1000 Genomes Reference Panel:
  - Bref3 files: [1000 Genomes Bref3](https://bochet.gcc.biostat.washington.edu/beagle/1000_Genomes_phase3_v5a/b37.bref3/).
