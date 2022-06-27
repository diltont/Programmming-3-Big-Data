#!/bin/bash
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --partition=assemblix
#SBATCH --job-name=Dilton
#SBATCH --time 48:00:00
export NGSDB_R1=/data/dataprocessing/MinIONData/MG5267/MG5267_TGACCA_L008_R1_001_BC24EVACXX.filt.fastq
export NGSDB_R2=/data/dataprocessing/MinIONData/MG5267/MG5267_TGACCA_L008_R2_001_BC24EVACXX.filt.fastq
export OUTPUT=/students/2021-2022/master/Dilton_DSLS
[ -d ${OUTPUT}/output ] || mkdir ${OUTPUT}/output
[ -d output ] || mkdir output

#velveth directory hash_length {[-file_format][-read_type][-separate|-interleaved] filename1 [filename2 ...]} {...} [options]
#parallel -j16 "velveth ${OUTPUT}/output/{} {} -longPaired -fastq -separate ${NGSDB_R1} ${NGSDB_R2}" ::: 19 2 31
seq 19 2 31 | parallel -j16 "velveth ${OUTPUT}/output/{} {} -longPaired -fastq -separate ${NGSDB_R1} ${NGSDB_R2}"
# parallel -j16 'velvetg ${OUTPUT}/output/{} -cov_cutoff auto -exp_cov auto' ::: 19 2 31
seq 19 2 31 | parallel -j16 "velvetg ${OUTPUT}/output/{}"
python3 assignment4.py
rm -rf ${OUTPUT}/output