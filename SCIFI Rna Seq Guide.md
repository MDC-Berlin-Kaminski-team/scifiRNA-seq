## Installation

  

1.  Install:

    -   samtools
    -   STAR aligner, version 2.6.1d. If you install later versions, then you'll need to rebuild yourself STAR index files
    -   featureCounts
    -   scifiRNA-seq itself:  
    https://github.com/MDC-Berlin-Kaminski-team/scifiRNA-seq

2.  Download picard jar file (picard-2.19.2-CeMM-all.jar) from here:  
    [https://github.com/DanieleBarreca/picard/releases/2.19.2-CeMM](https://github.com/DanieleBarreca/picard/releases/2.19.2-CeMM)

  

  

## Demultiplexing

  

Creation of the demultiplexed BAM files out of the basecall files is the first step before one can run scifiRNA-seq pipeline. We'll need to run picard on Illumina directory.

  

The guide to running it is here:  
[https://github.com/epigen/scifiRNA-seq/blob/main/demultiplexing_guide.pdf](https://github.com/epigen/scifiRNA-seq/blob/main/demultiplexing_guide.pdf)

  

Note: the guide mentions an annotation file that is needed at the second run. This annotation file (in our example it is called "annotation.tsv" is TAB-delimited, not comma-delimited like in many other annotation files).

  

In our specific case we run:

  
```
java \
-Xmx20G \
-jar picard-2.19.2-CeMM-all.jar \
IlluminaBasecallsToMultiplexSam \
RUN_DIR=illumina \
LANE=001 \
OUTPUT=undemultiplexed.bam \
SEQUENCING_CENTER=BSF \
NUM_PROCESSORS=2 \
APPLY_EAMSS_FILTER=false \
INCLUDE_NON_PF_READS=false \
TMP_DIR=tmp \
CREATE_MD5_FILE=false \
FORCE_GC=false \
MAX_READS_IN_RAM_PER_TILE=9000000 \
MINIMUM_QUALITY=2 \
VERBOSITY=INFO \
QUIET=false \
VALIDATION_STRINGENCY=STRICT \
CREATE_INDEX=false \
GA4GH_CLIENT_SECRETS=client_secrets.json
```
  

  
```
java \
-Xmx20G \
-Djava.io.tmpdir=./tmp \
-jar picard-2.19.2-CeMM-all.jar \
IlluminaSamDemux \
INPUT=undemultiplexed.bam \
OUTPUT_DIR=picard-output \
OUTPUT_PREFIX=out \
LIBRARY_PARAMS=picard-annotations.tsv \
METRICS_FILE=output.metrics \
TMP_DIR=./tmp \
COMPRESSION_LEVEL=9 \
CREATE_MD5_FILE=true \
OUTPUT_FORMAT=bam \
BARCODE_TAG_NAME=BC \
BARCODE_QUALITY_TAG_NAME=QT \
MAX_MISMATCHES=1 \
MIN_MISMATCH_DELTA=1 \
MAX_NO_CALLS=2 \
MINIMUM_BASE_QUALITY=0 \
VERBOSITY=INFO \
QUIET=false \
VALIDATION_STRINGENCY=STRICT \
MAX_RECORDS_IN_RAM=500000 \
CREATE_INDEX=false \
GA4GH_CLIENT_SECRETS=client_secrets.json \
USE_JDK_DEFLATER=false \
USE_JDK_INFLATER=false \
DEFLATER_THREADS=4 \
MATCHING_THREADS=4 \
READ_STRUCTURE=8M13B8B16M79T \
TAG_PER_MOLECULAR_INDEX=RX \
TAG_PER_MOLECULAR_INDEX=r2
```
  

## Prepare for run

  

1.  Download and unpack STAR index files and genome annotations (gtf). For human + mouse run:  
    wget [https://cf.10xgenomics.com/supp/cell-exp/refdata-gex-GRCh38-and-mm10-2020-A.tar.gz](https://cf.10xgenomics.com/supp/cell-exp/refdata-gex-GRCh38-and-mm10-2020-A.tar.gz)  
      
    More genomes can be found here:  
    [https://support.10xgenomics.com/single-cell-gene-expression/software/downloads/latest?](https://support.10xgenomics.com/single-cell-gene-expression/software/downloads/latest?)  
    
2.  Copy `scifiRNA-seq/scifi/config/default.yaml` file into `config.yaml` and edit it:  
    Search and replace `sbatch` with `sh -c`.

-   Specify the path of STAR executable under star_exe.
-   Specify the location of star index files under `star_genome_dir`. In our example it would reside at:  
    `refdata-gex-GRCh38-and-mm10-2020-A/star/`
-   Specify the location of GTF file under `gtf_file`. In out example it is:  
    `refdata-gex-GRCh38-and-mm10-2020-A/genes/genes.gtf`
-   Specify the location of featureCounts executable under `featurecounts_exe`.

3.  Create sample annotation and round1 plate well annotation files.

## Run:  
Must be run within conda scifi environment:  

`nohup scifi map --input-bam-glob picard-output/out#{sample_name}.bam sample_annotation.csv -c config.yaml --nocluster --num-processes 8 &`

Must run outside of conda scifi environment:  

`mkdir final-output`  

`python /home/mhoichm/home/scifiRNA-seq/scifi/scripts/summarizer.py --cell-barcodes r2 --save-gene-expression --sample-name SCIFI_LIG384 --r1-attributes plate_well --r1-annot round1_plate_well_annotation.csv --r2-barcodes 737K-cratac-v1.reverse_complement.csv 'pipeline_output/SCIFI_LIG384/SCIFI_LIG384_*/*.ALL.STAR.Aligned.out.bam.featureCounts.bam' final-output/res`
     
`python /home/mhoichm/home/scifiRNA-seq/scifi/scripts/report.py final-output/res.metrics.csv.gz final-output/A01 --species-mixture`

`python ../../scifiRNA-seq/scifi/scripts/summarizer.py --cell-barcodes r2 --save-gene-expression --only-summary --sample-name SCIFI_LIG384 --r1-attributes plate_well --r1-annot round1_plate_well_annotation.csv --r2-barcodes 737K-cratac-v1.reverse_complement.csv 'pipeline_output/SCIFI_LIG384/SCIFI_LIG384_*/SCIFI_LIG384_*.ALL.STAR.Aligned.out.bam.featureCounts.bam' all-res/res`
