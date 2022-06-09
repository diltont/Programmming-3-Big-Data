#!/bin/bash 
#SBATCH --nodes=1
#SBATCH --time=0-07:00:00
#SBATCH --cpus-per-task=16
# job name
#SBATCH --job-name=Dilton
#SBATCH --partition=assemblix
mkdir -p output
export time_file=1
echo $time_file
export timings_txt=output/timings.txt
export time=/usr/bin/time
export BLASTDB=/local-fs/datasets/refseq_protein/refseq_protein
export blastoutput=output/blastoutput.txt

for cpu in {1..16} ; do $time -a -o $timings_txt -f %e blastp -query MCRA.faa -db $BLASTDB -num_threads $cpu -outfmt 6 >> $blastoutput ; done  
python3 assignment3.py