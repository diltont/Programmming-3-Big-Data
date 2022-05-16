# -*- coding: utf-8 -*-
"""
Created on Mon May  9 14:00:41 2022`````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

@author: User
"""
import multiprocessing as mp
from Bio import Entrez
import sys

Entrez.email = "diltont@gmail.com"  # Always tell NCBI who you are
Entrez.api_key='e598a6f590bf95f954151347dc2165126807'
# pmid = "30049270"
#pmid=sys.argv[1]
def eread(pmid):
  results = Entrez.read(Entrez.elink(dbfrom="pubmed",
                                   db="pmc",
                                   LinkName="pubmed_pmc_refs",
                                   id=pmid,
                                   api_key='e598a6f590bf95f954151347dc2165126807'))
  references = [f'{link["Id"]}' for link in results[0]["LinkSetDb"][0]["Link"]]
  
  return references


def efetch(pmid):
  handle = Entrez.efetch(db="pmc", id=pmid, rettype="XML", retmode="text",
                           api_key='e598a6f590bf95f954151347dc2165126807')
  with open(f'output/{pmid}.xml', 'wb') as file:
    file.write(handle.read())
  #handle.close()


if __name__ == "__main__":
  pmid=sys.argv[1]
  ids=eread(pmid)
  cpus = mp.cpu_count()
  with mp.Pool(cpus) as pool:
    pool.map(efetch, ids[0:10])
    



  