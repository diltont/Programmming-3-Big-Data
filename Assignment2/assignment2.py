# -*- coding: utf-8 -*-
"""
Created on Thu May 19 09:18:32 2022

@author: User
"""
import multiprocessing as mp
from multiprocessing.managers import BaseManager, SyncManager
from Bio import Entrez
import os, sys, time, queue
import argparse
import pickle
POISONPILL = "MEMENTOMORI"
ERROR = "DOH"
AUTHKEY = b'whathasitgotinitspocketsesss?'
data = ["Always", "look", "on", "the", "bright", "side", "of", "life!"]
Entrez.email = "diltont@gmail.com"  # Always tell NCBI who you are
Entrez.api_key='e598a6f590bf95f954151347dc2165126807'
def make_server_manager(port, authkey,ip):
    """ Create a manager for the server, listening on the given port.
        Return a manager object with get_job_q and get_result_q methods.
    """
    job_q = queue.Queue()
    result_q = queue.Queue()

    # This is based on the examples in the official docs of multiprocessing.
    # get_{job|result}_q return synchronized proxies for the actual Queue
    # objects.
    class QueueManager(BaseManager):
        pass

    QueueManager.register('get_job_q', callable=lambda: job_q)
    QueueManager.register('get_result_q', callable=lambda: result_q)

    manager = QueueManager(address=(ip, port), authkey=authkey)
    manager.start()
    print('Server started at port %s' % port)
    return manager
  

def runserver(fn, data,port,ip):
    # Start a shared manager server and access its queues
    manager = make_server_manager(port, b'whathasitgotinitspocketsesss?',ip)
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()
    
    if not data:
        print("Gimme something to do here!")
        return
    
    print("Sending data!")
    for d in data:
        shared_job_q.put({'fn' : fn, 'arg' : d})
    
    time.sleep(2)  
    
    results = []
    while True:
        try:
            result = shared_result_q.get_nowait()
            results.append(result)
            print("Got result!", result)
            if len(results) == len(data):
                print("Got all results!")
                break
        except queue.Empty:
            time.sleep(1)
            continue
    # Tell the client process no more data will be forthcoming
    print("Time to kill some peons!")
    shared_job_q.put(POISONPILL)
    # Sleep a bit before shutting down the server - to give clients time to
    # realize the job queue is empty and exit in an orderly way.
    time.sleep(5)
    print("Aaaaaand we're done for the server!")
    manager.shutdown()
    print(results)
    

def make_client_manager(ip, port, authkey):
    """ Create a manager for a client. This manager connects to a server on the
        given address and exposes the get_job_q and get_result_q methods for
        accessing the shared queues from the server.
        Return a manager object.
    """
    class ServerQueueManager(BaseManager):
        pass

    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')

    manager = ServerQueueManager(address=(ip, port), authkey=authkey)
    manager.connect()

    print('Client connected to %s:%s' % (ip, port))
    return manager
def argsparser():
  parser=argparse.ArgumentParser(description='Download authors of a pubmed ID')
  parser.add_argument('-c', '--client', action='store_true', help='Connect to the Clients')
  parser.add_argument('-s', '--server', action='store_true', help='Connect to the Server')
  parser.add_argument('--host',type=str , action='store', help='Host')
  parser.add_argument('--port',dest='port',type=int,action='store',help='port')
  parser.add_argument('-n','--number_of_peons',dest='n',type=int)
  parser.add_argument('-a','--number_of_articles_to_download',dest='a',type=int,help='number of clients')
  parser.add_argument("pmid", type=int,nargs=1, help="Pubmed ID of the article")
  return parser.parse_args()
  
  
  

  
def eread(pmid):
  results = Entrez.read(Entrez.elink(dbfrom="pubmed",
                                   db="pubmed",
                                   LinkName="pubmed_pmc_refs",
                                   id=pmid,
                                   api_key='e598a6f590bf95f954151347dc2165126807'))
  
  references = [f'{link["Id"]}' for link in results[0]["LinkSetDb"][0]["Link"]]
  
  return references

def authors(pmid):
  try :
        handle = Entrez.esummary(db="pubmed", id=pmid)
        record = Entrez.read(handle)
        info =tuple(record[0]['AuthorList'])
  except RuntimeError:
        info = (None)
        print(f'Not fount {pmid}') 
    
  with open(f'output/{pmid}.authors.pickle', 'wb') as handle:
            pickle.dump(info, handle, protocol=pickle.HIGHEST_PROTOCOL)
  handle1 = Entrez.efetch(db="pmc", id=pmid, rettype="XML", retmode="text",
                           api_key='e598a6f590bf95f954151347dc2165126807')
  with open(f'output/{pmid}.xml', 'wb') as file:
    file.write(handle1.read())
  
  return
  
  
  
def runclient(num_processes,ip,port):
    manager = make_client_manager(ip, port, AUTHKEY)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    run_workers(job_q, result_q, num_processes)
    
def run_workers(job_q, result_q, num_processes):
    processes = []
    for p in range(num_processes):
        temP = mp.Process(target=peon, args=(job_q, result_q))
        processes.append(temP)
        temP.start()
    print("Started %s workers!" % len(processes))
    for temP in processes:
        temP.join()

def peon(job_q, result_q):
    my_name = mp.current_process().name
    while True:
        try:
            job = job_q.get_nowait()
            if job == POISONPILL:
                job_q.put(POISONPILL)
                print("Aaaaaaargh", my_name)
                return
            else:
                try:
                    result = job['fn'](job['arg'])
                    print("Peon %s Workwork on %s!" % (my_name, job['arg']))
                    result_q.put({'job': job, 'result' : result})
                except NameError:
                    print("Can't find yer fun Bob!")
                    result_q.put({'job': job, 'result' : ERROR})

        except queue.Empty:
            print("sleepytime for", my_name)
            time.sleep(1)


if __name__ == "__main__":
  #assignment2.py -n <number_of_peons_per_client> [-c | -s] --port <portnumber> --host <serverhost> 
  #-a <number_of_articles_to_download> STARTING_PUBMED_ID
  #pmid='30049270'
  args=argsparser()
  pmid=args.pmid
  ip=args.host
  port=args.port
  references=eread(pmid)
  number=args.a
  references=references[:number]
  
  authors(pmid)
  if args.server:
    server = mp.Process(target=runserver, args=(authors, references,port,ip))
    server.start()
    time.sleep(1)
    server.join()
  if args.client:
    n_peons=args.n
    client = mp.Process(target=runclient, args=(n_peons,ip,port))
    client.start()
    client.join()
    
#30049270