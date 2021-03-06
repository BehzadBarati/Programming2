#Import libraries
import sys
from Bio import Entrez
import multiprocessing as mp


#Assumptions and settings
pmid = sys.argv[1]        #ID of our targeted paper
email = "b.barati@st.hanze.nl"
Entrez.api_key = "23aedd7722b207ebc97bc37707b399bfb009"


#Function for returning 10 refrences of selected paper
def get_citations(paper_id):
    Entrez.email = email 
    records = Entrez.read(Entrez.elink(dbfrom="pubmed", id=paper_id, linkname="pubmed_pubmed_refs"))
    pmc_ids = [link["Id"] for link in records[0]["LinkSetDb"][0]["Link"]]
    if len(pmc_ids) > 11: 
        return [str(p) for p in pmc_ids[1:11]]
    else:
        return [str(p) for p in pmc_ids]

#Function to save a paper based on IDs
def get_papers(paper_id):
    Entrez.email = email 
    fetch = Entrez.efetch(db='pubmed',resetmode='xml', id=paper_id, rettype='full')
    with open(f'output/PUBMED_{paper_id}.xml', 'wb') as f:
        f.write(fetch.read())


if __name__ == "__main__":
    cpus = mp.cpu_count()
    paper_ids = get_citations(pmid)
    
    with mp.Pool(cpus) as pool:
        pool.map(get_papers, paper_ids)