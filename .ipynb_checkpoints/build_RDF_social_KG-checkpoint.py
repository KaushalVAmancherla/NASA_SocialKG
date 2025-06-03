#!/usr/bin/env python
# coding: utf-8

# In[1]:


from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, RDFS, OWL, XSD

from helper_funcs import *
from constants import *


# In[2]:


g = Graph()


# In[3]:


g.parse(ontology_fp, format="xml")


# In[4]:


ont_iri = g.value(None, RDF.type, OWL.Ontology)
print("\nOntology IRI:", ont_iri)


# In[5]:


ONTO = Namespace(ont_iri + "#")
g.bind("onto", ONTO)


# In[6]:


DATA = Namespace(ont_iri + "/data/")
g.bind("data", DATA)


# In[7]:


print("XSD base URI:", str(XSD))
print("OWL base URI:", str(OWL))
print("RDF base URI:", str(RDF))
print("DATA base URI:", str(DATA))
print("ONTO base URI:", str(ONTO))


# In[8]:


g.bind("rdf",  RDF)
g.bind("rdfs", RDFS)
g.bind("owl",  OWL)
g.bind("xsd",  XSD)


# In[9]:


raw_openalex_author_data = read_json_from_local(person_raw_openalex_data_fp)
raw_openalex_works_data = read_json_from_local(works_raw_openalex_data_fp)
author_to_works_ids = read_json_from_local(person_to_works_id_jpl_fp)


# In[10]:


###### HELPER FUNCTIONS #######
def mint_uri(*segments: str):
    """
    entity_type: e.g. "person" or "organization"
    local_id: the unique ID (e.g. OpenAlex ID without the URL prefix)
    """
    path = "/".join(segments)
    return URIRef(DATA + path)


# In[17]:


def get_parsed_openalex_id(oa_id):
    return oa_id.rsplit("/", 1)[-1]


# In[11]:


def add_citation_properties(node_uri,node_id,counts_metadata,node_type):
    for rec in counts_metadata:
        yr = rec["year"]
    
        # Mint a URI for this year's citation metadata
        cnode = mint_uri(f"{node_type}/{node_id}/citationCountMetadata", str(yr))
    
        g.add((cnode, RDF.type, ONTO.CitationCountMetadata))
        g.add((cnode, ONTO.year,     Literal(yr, datatype=XSD.gYear)))
        g.add((cnode, ONTO.quantity, Literal(rec["cited_by_count"], datatype=XSD.integer)))
        g.add((node_uri, ONTO.hasCitationData, cnode))


# In[12]:


def add_works_properties(node_uri,node_id,counts_metadata,node_type):
    for rec in counts_metadata:
        
        yr = rec["year"]
    
        # Mint a URI for this year's works metadata
        wnode = mint_uri(f"{node_type}/{node_id}/worksCountMetadata", str(yr))

        g.add((wnode, RDF.type, ONTO.WorksCountMetadata))
        g.add((wnode, ONTO.year,     Literal(yr, datatype=XSD.gYear)))
        g.add((wnode, ONTO.quantity, Literal(rec["works_count"], datatype=XSD.integer)))
        g.add((node_uri, ONTO.hasWorksData, wnode))


# In[13]:


###### GLOBAL CACHE FOR INSTITUTION DATA ######
global_inst_cache = {} #id -> URI for institution


# In[14]:


def process_institution(i_id,inst_uri,metadata):
    # 1) mint and type the org node
    full_id  = metadata["id"]

    # 2) core metadata
    g.add((inst_uri, ONTO.openalex_id, Literal(full_id, datatype=XSD.string)))
    
    display_name = metadata["display_name"]
    g.add((inst_uri, ONTO.name, Literal(display_name)))
    
    # 3) flat data properties
    c = metadata.get("cited_by_count")
    g.add((inst_uri, ONTO.citedByCount, Literal(c, datatype=XSD.int)))
    
    if "grants_count" in metadata:
        grants = metadata["grants_count"]
        
        g.add((inst_uri, ONTO.grantsCount, Literal(grants, datatype=XSD.int)))

    # 4) summary_stats
    stats = metadata.get("summary_stats", {})
    
    if mean2 := stats.get("2yr_mean_citedness"):
        g.add((inst_uri, ONTO.twoYearMeanCitedness,Literal(mean2, datatype=XSD.double)))
        
    if h := stats.get("h_index"):
        g.add((inst_uri, ONTO.hIndex, Literal(h, datatype=XSD.int)))
        
    if i10 := stats.get("i10_index"):
        g.add((inst_uri, ONTO.i10Index, Literal(i10, datatype=XSD.int)))

    # 5) role-based works counts
    for role in metadata.get("roles", []):
        r = role.get("role")
        wc = role.get("works_count")
            
        if r == "publisher":
            g.add((inst_uri, ONTO.publisher_works_count,Literal(wc, datatype=XSD.int)))
        elif r == "funder":
            g.add((inst_uri, ONTO.funder_works_count,Literal(wc, datatype=XSD.int)))
        elif r == "institution":
            g.add((inst_uri, ONTO.institution_works_count,Literal(wc, datatype=XSD.int)))

    counts = metadata['counts_by_year']
    
    add_citation_properties(inst_uri,i_id,counts,"organization")
    add_works_properties(inst_uri,i_id,counts,"organization")


# In[15]:


def get_institution_metadata(institution_id):
    url = f"{base_url}/institutions/{institution_id}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error:", response.status_code)
        return None


# In[ ]:


def process_affiliations(person_uri,affiliations,isForLast):
    for affiliation in affiliations: 
        if not isForLast:
            full_id = affiliation['institution']['id']
            years = affiliation.get("years", [])
        else:
            full_id = affiliation['id']
            
        parsed_id = get_parsed_openalex_id(full_id)#full_id.rsplit("/", 1)[-1]
        person_local = str(person_uri).rsplit("/", 1)[-1]
        
        #1. ADD NODE TO GRAPH IF NOT ALREADY THERE
        if parsed_id not in global_inst_cache:
            inst_type = raw_openalex_inst_data[parsed_id]['inst_type']
            inst_uri = mint_uri("organization" if inst_type == ORG else "group",parsed_id)

            #add node into graph
            g.add((
                inst_uri,
                RDF.type,
                ONTO.Organization if inst_type == ORG else ONTO.Group
            ))

            #add the institution metadata to this node (data property, we only need to do this one time upon init)
            metadata = get_institution_metadata(parsed_id)
            process_institution(parsed_id,inst_uri,metadata)

            #add to global cache
            global_inst_cache[parsed_id] = inst_uri
            
        inst_uri = global_inst_cache[parsed_id]
        
        #2. create the edges with the Affiliation class capturing the temporal dependencies
        if isForLast: #simply add (Person) -lastKnownInstitution-> global_inst_cache[parsed_id]
            g.add((person_uri, ONTO.lastKnownInstitution, inst_uri))
            continue

        g.add((person_uri, ONTO.worksFor, inst_uri))
        
        #otherwise, we default to this case:
        #3. (Person) -worksFor-> Organization
        years = sorted(set(years))
        segments = []
        
        if years:
            seg_start = seg_prev = years[0]
            for y in years[1:]:
                if y == seg_prev + 1:
                    # still contiguous
                    seg_prev = y
                else:
                    # end of a run
                    segments.append((seg_start, seg_prev))
                    seg_start = seg_prev = y
                    
            segments.append((seg_start, seg_prev))
    
        # 3b.iii) for each segment, mint a distinct Affiliation node
        for start_year, end_year in segments:
            # URI-safe suffix, e.g. "2017-2019" or "2015"
            period = f"{start_year}-{end_year}" if start_year != end_year else f"{start_year}"
            aff_uri = mint_uri("affiliation", person_local, parsed_id, period)
        
            # a) type
            if (aff_uri, RDF.type, ONTO.Affiliation) not in g:
                g.add((aff_uri, RDF.type, ONTO.Affiliation))
        
            # b) timestamp props
            g.add((aff_uri, ONTO.affiliationStartYear,
                   Literal(start_year, datatype=XSD.gYear)))
            g.add((aff_uri, ONTO.affiliationEndYear,
                   Literal(end_year,   datatype=XSD.gYear)))
        
            g.add((person_uri, ONTO.hasAffiliation, aff_uri))
            
            if (aff_uri, ONTO.affiliatedOrganization, inst_uri) not in g:
                g.add((aff_uri, ONTO.affiliatedOrganization, inst_uri))


# In[ ]:


def parse_author_metadata(author,metadata_dict):
    ####### 1. CREATE PERSON NODE AND ADD THE SIMPLE PERSON DATA PROPERTIES ######
    local_id  = metadata_dict['id'].rsplit("/", 1)[-1]
    
    person_uri = mint_uri("person", local_id)
    #print(person_uri)
 
    g.add((person_uri, RDF.type, ONTO.Person)) #init node of type Person

    #attach data properties: openalexId, name, worksCount, citedByCount
    display_name = metadata_dict['display_name']
    works_count = metadata_dict['works_count']
    cited_by_count = metadata_dict['cited_by_count']
    
    g.add((person_uri, ONTO.openalex_id, Literal(local_id, datatype=XSD.string)))
    g.add((person_uri, ONTO.name, Literal(display_name)))
    g.add((person_uri, ONTO.openalex_id, Literal(local_id, datatype=XSD.string)))
    g.add((person_uri, ONTO.worksCount, Literal(works_count, datatype=XSD.integer)))
    g.add((person_uri, ONTO.citedByCount, Literal(cited_by_count, datatype=XSD.integer)))

    summary_stats = metadata_dict["summary_stats"]

    # 1. Two‐year mean citedness → xsd:double
    if "2yr_mean_citedness" in summary_stats:
        g.add((person_uri,ONTO["2yr_mean_citedness"],Literal(summary_stats["2yr_mean_citedness"], datatype=XSD.double)))
    
    # 2. h‐index → xsd:integer
    if "h_index" in summary_stats:
        g.add((person_uri,ONTO.h_index,Literal(summary_stats["h_index"], datatype=XSD.integer)))
    
    # 3. i10‐index → xsd:integer
    if "i10_index" in summary_stats:
        g.add((person_uri,ONTO.i10_index,Literal(summary_stats["i10_index"], datatype=XSD.integer)))

    #### ADDING YEARLY WORKS AND YEARLY CITATIONS ####
    counts = metadata_dict['counts_by_year']
    
    add_citation_properties(person_uri,local_id,counts,"person")
    add_works_properties(person_uri,local_id,counts,"person")

    ##### PARSING INSTITUTION AFFILIATIONS ######
    '''
    This will require first instantiating an institution as either an organization or a group, then
    we attach the associated metadata with it. However, it is important to check if the node for the institution already exists
    Basically, what we do is if a node is new, attach it into the graph, and then append the necessary metadata to it.

    Lastly, we must get a list of all institutions that are currently in the graph (this is simply the organizations class as group is a subclass
    of organizations). When we have these nodes, for a given institution we are currently processing, intersect the list of those graph nodes and
    the list of organizations with the target relation (i.e. child, parent, etc.) and establish the necessary edge (subGroupOf, etc.)
    Now, we must note not to duplicately add edges (i.e. if such an edge already exists, leave it).
    
    Once those nodes are populated, we add the edges of - (Person) -worksFor-> Organization  and - (Person) -lastKnownInstitution-> Organization with the
    associated time stamps via the Affiliation class
    '''
    affiliations = metadata_dict['affiliations']
    last_known = metadata_dict['last_known_institutions']
    
    process_affiliations(person_uri,affiliations,False)
    process_affiliations(person_uri,last_known,True)


# In[ ]:


##### PROCESSING WORKS DATA #####


# In[ ]:


global_paper_cache = {} #id -> URI for institution
global_project_cache = {} #id -> URI for project


# In[ ]:


def get_or_create_paper(work_id):
    #1. Initialize the Paper object
    p = raw_openalex_works_data[work_id]
    #print("P -> ", p)
    
    """
    p["id"]         → OpenAlex paper URL
    p["doi"]        → DOI 
    p["display_name"] → title
    p["publication_date"] → e.g. "2023-07-15"
    plus all the counts & percentiles...
    """

    local_id = get_parsed_openalex_id(p['id'])
    
    if local_id in global_inst_cache:
        return

    uri = mint_uri("paper", local_id)
    global_paper_cache[local_id] = uri

    g.add((uri, RDF.type, ONTO.Paper))
    
    g.add((uri, ONTO.citedByCount, Literal(p["cited_by_count"], datatype=XSD.int)))
    g.add((uri, ONTO.citesCount, Literal(p["referenced_works_count"], datatype=XSD.int)))
    g.add((uri, ONTO.name,       Literal(p["display_name"])))
    g.add((uri, ONTO.openalex_id, Literal(local_id, datatype=XSD.string)))
    g.add((uri, ONTO.producedDate,Literal(p["publication_date"], datatype=XSD.dateTime)))
    g.add((uri, ONTO.producedYear,Literal(p["publication_year"], datatype=XSD.gYear)))
    g.add((uri, ONTO.doi,Literal(p["doi"], datatype=XSD.string)))

    # 1. same‐subfield percentile (double)
    same_subfield_percentile = p["citation_normalized_percentile"]

    if same_subfield_percentile:
        val = same_subfield_percentile["value"]
        g.add((
            uri,
            ONTO.citation_percentile_year_same_subfield,
            Literal(val, datatype=XSD.double)
        ))
    
    # 2 & 3. all‐subfields percentile min & max (integers)
    cited_by_percentile_year = p["cited_by_percentile_year"]
    
    if cited_by_percentile_year:
        minv = cited_by_percentile_year["min"]
        maxv = cited_by_percentile_year["max"]
        g.add((
            uri,
            ONTO.citation_percentile_year_all_subfield_min,
            Literal(minv, datatype=XSD.int)
        ))
        g.add((
            uri,
            ONTO.citation_percentile_year_all_subfield_max,
            Literal(maxv, datatype=XSD.int)
        ))

    #Add yearly citation data count
    counts_metadata = p["counts_by_year"]
    node_type = "paper"
    
    add_citation_properties(uri,local_id,counts_metadata,node_type)

    return uri


# In[ ]:


for author,metadata_dict in raw_openalex_author_data.items():
    parse_author_metadata(author,metadata_dict)


# In[ ]:


# ───────────────────────────────────────────────────────────────────────────
# B.1. Create one Project node for each existing Paper.  Copy pub_date → startedOn,
#       and link Project –resultsIn→ Paper, plus Paper –outputOfProject→ Project.
# ───────────────────────────────────────────────────────────────────────────

for work_id, p in raw_openalex_works_data.items():
    # 1) Grab the Paper that was already created
    paper_uri = get_or_create_paper(work_id)

    # 1b) Build a “local_id” exactly as your get_or_create_paper did:
    local_id = get_parsed_openalex_id(p["id"])
    
    # 1c) Mint a Project URI that parallels the Paper’s URI
    project_uri = mint_uri("project", local_id)
    # If you already stored it in a cache, skip; otherwise:
    if project_uri not in global_inst_cache:
        global_project_cache[local_id] = project_uri  # or whatever your cache‐logic is

    # 1d) Declare it as a Project
    g.add((project_uri, RDF.type, ONTO.Project))

    # 1e) Copy over the “publication_date” → “startedOn”
    pub_date = p.get("publication_date")             # e.g. "2023-07-15"
    if pub_date:
        g.add((project_uri, ONTO.startedOn,
               Literal(pub_date, datatype=XSD.dateTime)))

    # 1f) Link Project → resultsIn → Paper
    g.add((project_uri, ONTO.resultsIn, paper_uri))

    # 1g) Link Paper → outputOfProject → Project
    g.add((project_uri, ONTO.outputOfProject, paper_uri))


# In[ ]:


# ─────────────────────────────────────────────────────────────────────────────
# 2. For each Paper, use its “authorships” to link:
#     • Person –worksOn→ Project  &  Person –participatesIn→ Project
#     • Group  –worksOn→ Project
#    Time‐stamp each such triple with Project.startedOn (i.e. the Paper’s pub_date).
# ─────────────────────────────────────────────────────────────────────────────


# In[ ]:


for work_id, p in raw_openalex_works_data.items():
    local_id = get_parsed_openalex_id(p["id"])
    
    # 1) Make sure this Paper has already been created:
    paper_uri = global_paper_cache.get(local_id)

    # 2) Build the Project URI (must match Step 1’s minting logic):
    project_uri = global_project_cache.get(local_id)
    if project_uri is None:
        # If you skipped creating this Project in Step 1, skip here too.
        continue

    pub_date = p.get("publication_date")

    for auth_info in p.get("authorships", []):
        full_author_url = auth_info["author"]["id"]          # e.g. "https://openalex.org/A5065824713"
        author_stripped  = get_parsed_openalex_id(full_author_url)    # e.g. "A5065824713"

        # 4a) Only if this author is one of the keys in author_to_works_ids do we proceed:
        if author_stripped not in author_to_works_ids:
            continue

        # 4b) Mint / look up the Person URI for that author:
        person_uri = mint_uri("person", author_stripped)
        # (we assume that Person was created earlier in your parse_works_metadata)

        # 4c) Link Person → worksOn → Project (with timestamp)
        g.add((person_uri, ONTO.worksOn, project_uri))
        
        # 4d) Mirror as participatesIn
        g.add((person_uri, ONTO.participatesIn, project_uri))

        # 4e) Now—**only for this SAME author**—create the Group → worksOn link for each institution
        #     that appears under this author’s “institutions” array.
        for inst in auth_info.get("institutions", []):
            full_inst_url = inst["id"]                       # e.g. "https://openalex.org/I2799356940"
            inst_stripped  = get_parsed_openalex_id(full_inst_url)        # e.g. "I2799356940"

            # Mint or look up that Group/Organization URI:
            inst_uri = mint_uri("organization", inst_stripped)

            if inst_stripped not in global_inst_cache:
                metadata = get_institution_metadata(parsed_id)
                process_institution(parsed_id,inst_uri,metadata)
                    
            # Add the triple Group → worksOn → Project (timestamped)
            g.add((inst_uri, ONTO.worksOn, project_uri))


# In[ ]:




