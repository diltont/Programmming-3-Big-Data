import pandas as pd
from Bio import SeqIO
import os
import shutil

def get_N50():
    contif_output = []
    N50_output = []
    for i in range (19,32,2):
        try:
            fasta_sequences = SeqIO.parse(open(f"/students/2021-2022/master/Dilton_DSLS/output/{i}/contigs.fa"),'fasta')
        except FileNotFoundError:
            print(f"{i}/contigs.fa not found.")
            contif_output.append(i)
            N50_output.append(None)
            continue
        data = []
        for fasta in fasta_sequences:
            name, sequence = fasta.id, str(fasta.seq)
            data.append(len(sequence))
        total_contig_length = sum(data)
        half_total_contig_length = total_contig_length / 2
        N50 = 0
        data = sorted(data,reverse = True)
        for j in data:
            if N50 < half_total_contig_length:
                N50 += j
            else:
                N50 = j
                break
        contif_output.append(i)
        N50_output.append(N50)
    return contif_output, N50_output


def to_csv(contif_output, N50_output):
    df = {'contif': contif_output, 'N50': N50_output} 
    df = pd.DataFrame(df).to_csv("output/output.csv")

def get_the_best_file():
    data = pd.read_csv("output/output.csv")
    contif = data[data.N50 == data.N50.max()].contif.tolist()[0]
    src_path = f"/students/2021-2022/master/Dilton_DSLS/output/{contif}/contigs.fa"
    dst_path = f"output"
    shutil.copy(src_path, dst_path)

if __name__ =="__main__":
    contif_output, N50_output = get_N50()
    if not os.path.exists("output"):
        os.makedirs("output")
    to_csv(contif_output, N50_output)
    get_the_best_file()
