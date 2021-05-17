import multiprocessing as mp
from multiprocessing.managers import BaseManager, SyncManager
import os, sys, time, queue
from Bio import Entrez


def make_server_manager(port, authkey):
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

    manager = QueueManager(address=('', port), authkey=authkey)
    manager.start()
    print('Server started at port %s' % port)
    return manager


def runserver(fn, data):
    # Start a shared manager server and access its queues
    manager = make_server_manager(PORTNUM, AUTHKEY)
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


#Function for returning 10 refrences of selected paper
def get_citations(paper_id, number_of_articles):
    Entrez.email = email 
    records = Entrez.read(Entrez.elink(dbfrom="pubmed", id=paper_id, linkname="pubmed_pubmed_refs", retmode="xml"))
    pmc_ids = [link["Id"] for link in records[0]["LinkSetDb"][0]["Link"]]
    print(pmc_ids)
    if len(pmc_ids) > number_of_articles: 
        return [str(p) for p in pmc_ids[1:number_of_articles]]
    else:
        return [str(p) for p in pmc_ids]

#Function to save a paper based on IDs
def get_papers(paper_id):
    Entrez.email = email 
    fetch = Entrez.efetch(db='pubmed',retmode='xml', id=paper_id, rettype='full')
    with open(f'output/PUBMED_{paper_id}.xml', 'wb') as f:
        f.write(fetch.read())



def runclient(num_processes):
    manager = make_client_manager(IP, PORTNUM, AUTHKEY)
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


#Assumptions and settings
email = "b.barati@st.hanze.nl"
Entrez.api_key = "23aedd7722b207ebc97bc37707b399bfb009"
POISONPILL = "MEMENTOMORI"
ERROR = "DOH"
AUTHKEY = b'whathasitgotinitspocketsesss?'
# arguments to pass are :
    # 1- number of childerns in client
    # 2- if we run server or client
    # 3- portnumber of server/client
    # 4- IP of host
    # 5- number of articles that should be downloaded
    # 6- ID of main article in Pubmed

number_of_children = int(sys.argv[1])
serverOrclient = sys.argv[2]
PORTNUM = int(sys.argv[3])
IP = sys.argv[4]
number_of_articles = int(sys.argv[5])
pmid = sys.argv[6]    
#pmid = 31508499


if __name__ == '__main__':
    
    if serverOrclient == 's':
        paper_ids = get_citations(pmid, number_of_articles)
        server = mp.Process(target=runserver, args=(get_papers, paper_ids))
        server.start()
        time.sleep(5)
        
    if serverOrclient == 'c':
        client = mp.Process(target=runclient, args=(number_of_children,))
        client.start()
        client.join()
