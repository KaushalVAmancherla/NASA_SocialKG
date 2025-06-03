#!/usr/bin/env python
# coding: utf-8

# In[1]:


from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, RDFS, OWL, XSD

from helper_funcs import *
from constants import *

import requests


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
    entity_type: e.g. "person" or "institution"
    local_id: the unique ID (e.g. OpenAlex ID without the URL prefix)
    """
    path = "/".join(segments)
    return URIRef(DATA + path)


# In[11]:


def get_parsed_openalex_id(oa_id):
    return oa_id.rsplit("/", 1)[-1]


# In[12]:


def add_citation_properties(node_uri,node_id,counts_metadata,node_type):
    for rec in counts_metadata:
        yr = rec["year"]
    
        # Mint a URI for this year's citation metadata
        cnode = mint_uri(f"{node_type}/{node_id}/citationCountMetadata", str(yr))
    
        g.add((cnode, RDF.type, ONTO.CitationCountMetadata))
        g.add((cnode, ONTO.year,     Literal(yr, datatype=XSD.gYear)))
        g.add((cnode, ONTO.quantity, Literal(rec["cited_by_count"], datatype=XSD.integer)))
        g.add((node_uri, ONTO.hasCitationData, cnode))


# In[13]:


def add_works_properties(node_uri,node_id,counts_metadata,node_type):
    for rec in counts_metadata:
        
        yr = rec["year"]
    
        # Mint a URI for this year's works metadata
        wnode = mint_uri(f"{node_type}/{node_id}/worksCountMetadata", str(yr))

        g.add((wnode, RDF.type, ONTO.WorksCountMetadata))
        g.add((wnode, ONTO.year,     Literal(yr, datatype=XSD.gYear)))
        g.add((wnode, ONTO.quantity, Literal(rec["works_count"], datatype=XSD.integer)))
        g.add((node_uri, ONTO.hasWorksData, wnode))


# In[14]:


###### GLOBAL CACHE FOR INSTITUTION DATA ######
global_inst_cache = {} #id -> URI for institution
institution_metadata_dict = {} #id -> metadata


# In[15]:


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
    
    add_citation_properties(inst_uri,i_id,counts,"institution")
    add_works_properties(inst_uri,i_id,counts,"institution")


# In[16]:


def get_institution_metadata(institution_id):
    url = f"{base_url}/institutions/{institution_id}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error:", response.status_code)
        return None


# In[17]:


def get_topic_metadata(topic_id):
    url = f"{base_url}/topics/{topic_id}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error:", response.status_code)
        return None


# In[18]:


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
            inst_uri = mint_uri("institution",parsed_id)

            #add node into graph
            g.add((inst_uri,RDF.type,ONTO.Institution))

            #add the institution metadata to this node (data property, we only need to do this one time upon init)
            metadata = get_institution_metadata(parsed_id)
            institution_metadata_dict[parsed_id] = metadata
            
            process_institution(parsed_id,inst_uri,metadata)

            #add to global cache
            global_inst_cache[parsed_id] = inst_uri
            
        inst_uri = global_inst_cache[parsed_id]
        
        #2. create the edges with the Affiliation class capturing the temporal dependencies
        if isForLast: #simply add (Person) -lastKnownInstitution-> global_inst_cache[parsed_id]
            g.add((person_uri, ONTO.lastKnownInstitution, inst_uri))
            continue
        
        #otherwise, we default to this case:
        #3. (Person) -worksFor-> institution
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
            acc_uri = mint_uri("association", person_local, parsed_id, period)
        
            # type
            if (acc_uri, RDF.type, ONTO.Association) not in g:
                g.add((acc_uri, RDF.type, ONTO.Association))
        
            # Attach startDate & endDate to that same node
            g.add((acc_uri, ONTO.startDate, Literal(f"{start_year}-01-01T00:00:00", datatype=XSD.date)))
            g.add((acc_uri, ONTO.endDate,   Literal(f"{end_year}-12-31T23:59:00", datatype=XSD.date)))

            # Link Person and Institution through that Association
            g.add((person_uri, ONTO.hasAssociation, acc_uri))
            g.add((acc_uri, ONTO.associatedInstitution, inst_uri))


# In[19]:


def parse_author_metadata(author,metadata_dict):
    ####### 1. CREATE PERSON NODE AND ADD THE SIMPLE PERSON DATA PROPERTIES ######
    local_id  = get_parsed_openalex_id(metadata_dict['id']) #metadata_dict['id'].rsplit("/", 1)[-1]
    
    person_uri = mint_uri("person", local_id)
    #print(person_uri)
 
    g.add((person_uri, RDF.type, ONTO.Person)) #init node of type Person

    #attach data properties: openalexId, name, worksCount, citedByCount
    display_name = metadata_dict['display_name']
    works_count = metadata_dict['works_count']
    cited_by_count = metadata_dict['cited_by_count']
    
    g.add((person_uri, ONTO.openalex_id, Literal(local_id, datatype=XSD.string)))
    g.add((person_uri, ONTO.name, Literal(display_name)))
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
    This will require first instantiating an institution and we attach the associated metadata with it. However, it is important to check if the node for the institution already exists
    Basically, what we do is if a node is new, attach it into the graph, and then append the necessary metadata to it.
    
    Once those nodes are populated, we add the edges of - (Person) -worksFor-> Institution  and - (Person) -lastKnownInstitution-> Institution with the
    associated time stamps via the Affiliation class
    '''
    affiliations = metadata_dict['affiliations']
    last_known = metadata_dict['last_known_institutions']
    
    process_affiliations(person_uri,affiliations,False)
    process_affiliations(person_uri,last_known,True)


# In[20]:


for author,metadata_dict in raw_openalex_author_data.items():
    parse_author_metadata(author,metadata_dict)


# In[21]:


##### PROCESSING WORKS DATA #####


# In[22]:


global_paper_cache = {} #id -> URI for institution
global_project_cache = {} #id -> URI for project
global_topic_cache = {} #id -> URI for topic

topic_metadata_dict = {} #id -> topic_metadata


# In[23]:


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


# In[24]:


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
        g.add((project_uri, ONTO.startingDate, Literal(pub_date, datatype=XSD.dateTime)))

    # 1f) Link Project → resultsIn → Paper
    g.add((project_uri, ONTO.resultsIn, paper_uri))

    # 1g) Link Paper → outputOfProject → Project
    g.add((project_uri, ONTO.outputOfProject, paper_uri))


# In[25]:


# ─────────────────────────────────────────────────────────────────────────────
# 2. For each Paper, use its “authorships” to link:
#     • Person –worksOn→ Project  &  Person –participatesIn→ Project
#     • Group  –worksOn→ Project
#    Time‐stamp each such triple with Project.startedOn (i.e. the Paper’s pub_date).
# ─────────────────────────────────────────────────────────────────────────────


# In[26]:


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

    for topic_entry in p.get("topics", []):
        # a) Build or look up the Topic URI
        topic_id  = get_parsed_openalex_id(topic_entry["id"])  # e.g. “T10655”
        topic_uri = mint_uri("topic", topic_id)
        score     = topic_entry.get("score", 0.0)

        # b) If this Topic node doesn’t exist yet, create it
        if topic_id not in global_topic_cache: #(topic_uri, RDF.type, ONTO.Topic) not in g:
            g.add((topic_uri, RDF.type, ONTO.Topic))
            g.add((topic_uri, ONTO.displayName, Literal(topic_entry["display_name"])))

            topic_metadata_dict[topic_id] = get_topic_metadata(topic_id)
            global_topic_cache[topic_id] = topic_uri
            
        topic_metadata = topic_metadata_dict[topic_id]
        
        # 1. If there is a description field, attach it
        desc = topic_metadata.get("description")
        if desc:
            g.add((topic_uri, ONTO.description, Literal(desc)))
    
        # 2. If there is a keywords array, attach each keyword’s display_name
        for kw in topic_metadata.get("keywords", []):
            # each kw looks like {"id": "...", "display_name": "...", "score": ...}
            if kw:
                g.add((topic_uri, ONTO.keyword, Literal(kw)))
        
        # i) Mint a unique Confidence URI
        conf_id    = f"{local_id}_{topic_id}"
        conf_uri   = mint_uri("confidence", conf_id)

        g.add((paper_uri, ONTO.publishedOn,   topic_uri))
        
        # ii) Type it and link to Project & Topic
        g.add((project_uri, ONTO.has,   topic_uri))
        
        g.add((conf_uri, RDF.type,              ONTO.Confidence))
        g.add((conf_uri, ONTO.linkSourceProject, project_uri))
        g.add((conf_uri, ONTO.linkTargetTopic,   topic_uri))

        g.add((conf_uri, ONTO.confidenceScore, Literal(score, datatype=XSD.double)))
        
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

        # 4d) Now—**only for this SAME author**—create the Group → worksOn link for each institution
        #     that appears under this author’s “institutions” array.
        inst_uris = set()
        
        for inst in auth_info.get("institutions", []):
            full_inst_url = inst["id"]                       # e.g. "https://openalex.org/I2799356940"
            inst_stripped  = get_parsed_openalex_id(full_inst_url)        # e.g. "I2799356940"

            # Mint or look up that Group/Institution URI:
            inst_uri = mint_uri("institution", inst_stripped)

            if inst_stripped not in global_inst_cache:
                metadata = get_institution_metadata(inst_stripped)
                institution_metadata_dict[inst_stripped] = metadata
                
                process_institution(inst_stripped,inst_uri,metadata)
                global_inst_cache[inst_stripped] = inst_uri
                    
            # Add the triple Group → worksOn → Project (timestamped)
            if ((inst_uri, ONTO.worksOn, project_uri)) not in g:
                g.add((inst_uri, ONTO.worksOn, project_uri))
                inst_uris.add(inst_uri)

            if ((inst_uri, ONTO.producesOutput, paper_uri)) not in g:
                g.add((inst_uri, ONTO.producesOutput, paper_uri))

            if ((inst_uri, ONTO.producesOutput, paper_uri)) not in g:
                g.add((paper_uri, ONTO.producedBy, inst_uri))

            inst_uris.add(inst_uri)

        # --- PRODUCES OUTPUT / PRODUCED BY --- #
        # Person → producesOutput → Paper  &  Paper → producedBy → Person
        g.add((person_uri, ONTO.producesOutput, paper_uri))
        g.add((paper_uri, ONTO.producedBy,     person_uri))
            
        # ----------------------------------------
        # C) PERSON/INSTITUTION → TOPIC
        # ----------------------------------------

        for inst_uri in inst_uris:
            # (inst_uri is something like mint_uri("institution", inst_stripped))
            for topic_entry in p.get("topics", []):
                topic_id  = get_parsed_openalex_id(topic_entry["id"])   # e.g. "T10655"
                topic_uri = mint_uri("topic", topic_id)                 # must already exist or have been created above
        
                # Person → associatedWithTopic → Topic (only if not already present)
                if (person_uri, ONTO.associatedWithTopic, topic_uri) not in g:
                    g.add((person_uri, ONTO.associatedWithTopic, topic_uri))
        
                # Institution → institutionAssociatedWithTopic → Topic (only if not already present)
                if (inst_uri, ONTO.institutionAssociatedWithTopic, topic_uri) not in g:
                    g.add((inst_uri, ONTO.institutionAssociatedWithTopic, topic_uri))

    # ----------------------------------------------------
    # D) CITATIONS: link Paper A → referencesOrCites → B
    # ----------------------------------------------------
    for ref_full in p.get("referenced_works", []):
        ref_id  = get_parsed_openalex_id(ref_full)
        ref_uri = global_paper_cache.get(ref_id)
        
        if ref_uri:
            # Link this paper to the referenced paper
            if (paper_uri, ONTO.referencesOrCites, ref_uri) not in g:
                g.add((paper_uri, ONTO.referencesOrCites, ref_uri))

            # Also link this paper to each author of the referenced paper
            for auth2 in raw_openalex_works_data.get(ref_full, {}).get("authorships", []):
                auth2_id  = get_parsed_openalex_id(auth2["author"]["id"])
                auth2_uri = mint_uri("person", auth2_id)
                if (auth2_uri, RDF.type, ONTO.Person) in g:
                    if (paper_uri, ONTO.referencesOrCites, auth2_uri) not in g:
                        g.add((paper_uri, ONTO.referencesOrCites, auth2_uri))

    # ----------------------------------------------------
    # E) RELATED‐TO‐PAPER: link Paper A → relatedToPaper → Z
    # ----------------------------------------------------
    
    # 1) Collect all Person URIs who workOn this project
    for rel_full in p.get("related_works", []):
        rel_id  = get_parsed_openalex_id(rel_full)
        rel_uri = global_paper_cache.get(rel_id)
        if rel_uri:
            if (paper_uri, ONTO.relatedToPaper, rel_uri) not in g:
                g.add((paper_uri, ONTO.relatedToPaper, rel_uri))

    persons_here = [
        uri for uri in g.subjects(ONTO.worksOn, project_uri)
        if (uri, RDF.type, ONTO.Person) in g
    ]

    # 2) Collect all Institution/Group URIs who workOn this project
    insts_here = [
        uri for uri in g.subjects(ONTO.worksOn, project_uri)
        if ((uri, RDF.type, ONTO.Institution) in g) or ((uri, RDF.type, ONTO.Group) in g)
    ]

    # F.1) Between every pair of persons, add collaboratesWithPartner
    n = len(persons_here)
    for i in range(n):
        A = persons_here[i]
        for j in range(i+1, n):
            B = persons_here[j]
            if (A, ONTO.collaboratesWithPartner, B) not in g:
                g.add((A, ONTO.collaboratesWithPartner, B))
            if (B, ONTO.collaboratesWithPartner, A) not in g:
                g.add((B, ONTO.collaboratesWithPartner, A))

    # F.2) Between each person and each institution, add collaboratesWithInstitution & hasContributor
    for A in persons_here:
        for G in insts_here:
            if (A, ONTO.collaboratesWithInstitution, G) not in g:
                g.add((A, ONTO.collaboratesWithInstitution, G))
            if (G, ONTO.hasContributor, A) not in g:
                g.add((G, ONTO.hasContributor, A))


# In[27]:


# ----------------------------------------------------------
# G) Build “isConnected” edges in one final pass over g
# ----------------------------------------------------------
# G.1) via co-author (collaboratesWithPartner)
for A, B in g.subject_objects(ONTO.collaboratesWithPartner):
    if (A, ONTO.isConnected, B) not in g:
        g.add((A, ONTO.isConnected, B))

# G.2) via citation path: Person A → producesOutput Q → referencesOrCites B
for A in g.subjects(RDF.type, ONTO.Person):
    for Q in g.objects(A, ONTO.producesOutput):
        for B in g.objects(Q, ONTO.referencesOrCites):
            if (B, RDF.type, ONTO.Person) in g:
                if (A, ONTO.isConnected, B) not in g:
                    g.add((A, ONTO.isConnected, B))

# G.3) via related‐paper path: Person A → producesOutput Q → relatedToPaper Z → producedBy B
for A in g.subjects(RDF.type, ONTO.Person):
    for Q in g.objects(A, ONTO.producesOutput):
        for Z in g.objects(Q, ONTO.relatedToPaper):
            for B in g.subjects(ONTO.producesOutput, Z):
                if (B, RDF.type, ONTO.Person) in g:
                    if (A, ONTO.isConnected, B) not in g:
                        g.add((A, ONTO.isConnected, B))


# In[28]:


for inst_id, inst_uri in global_inst_cache.items():
    metadata = institution_metadata_dict.get(inst_id)
    if not metadata:
        print("REACHED")
        continue

    for assoc in metadata.get("associated_institutions", []):
        # assoc["id"] is something like "https://openalex.org/I4210163724"
        assoc_id     = get_parsed_openalex_id(assoc["id"])   # "I4210163724"
        related_uri  = global_inst_cache.get(assoc_id)
        
        if related_uri is None:
            # if the parent/child institution wasn’t already inserted, skip it
            continue

        rel_type = assoc.get("relationship")
        if rel_type == "parent":
            # inst A has parent B  →  A —hasParentInstitution→ B
            if (inst_uri, ONTO.hasParentInstitution, related_uri) not in g:
                g.add((inst_uri, ONTO.hasParentInstitution, related_uri))

        else:
            # everything else (child, related, etc.)  →  A —relatedInstitution→ B
            if (inst_uri, ONTO.relatedInstitution, related_uri) not in g:
                g.add((inst_uri, ONTO.relatedInstitution, related_uri))


# In[29]:


for project_id, project_uri in global_project_cache.items():
    # 1) Find all “agents” who workOn this project
    #    We consider both Persons and Institutions (and Groups, if any).

    ########## coProduces ###########
    agents = set()
    for subj in g.subjects(ONTO.worksOn, project_uri):
        # Make sure subj is either a Person or an Institution (or Group)
        if (subj, RDF.type, ONTO.Person) in g or (subj, RDF.type, ONTO.Institution) in g:
            agents.add(subj)

    # 2) If fewer than 2 agents, nobody really “co‐produced.”
    if len(agents) < 2:
        continue

    # 3) Otherwise, each agent “coProduces” this project.
    #    (We do NOT attach a separate timestamp on the edge;
    #     you can always look up project’s ONTO.completedDate when querying.)
    for agent_uri in agents:
        if (agent_uri, ONTO.coProduces, project_uri) not in g:
            g.add((agent_uri, ONTO.coProduces, project_uri))


    ########## hasFunded ###########
    for grant in p.get("grants", []):
        print("GRANT -> ", grant)
        # grant["funding_institution"]["id"] is something like "https://openalex.org/I2799356940"
        funder = grant.get("funder", {})
        if not funder:
            continue
            
        funder_id = get_parsed_openalex_id(funder)  # e.g. "I2799356940"
        funder_uri = global_inst_cache.get(funder_id)
        if funder_uri is None:
            # If this institution wasn’t already created, skip it
            continue

        # Add (Institution) -hasFunded-> (Project), but only if it doesn’t exist yet
        if (funder_uri, ONTO.hasFunded, project_uri) not in g:
            g.add((funder_uri, ONTO.hasFunded, project_uri))


# In[30]:


for topic_id, topic_uri in global_topic_cache.items():
    metadata = topic_metadata.get(topic_id)
    if not metadata:
        continue

    for sib in metadata.get("siblings", []):
        # 1) Pull off the sibling’s OpenAlex ID (stripped)
        sib_full = sib.get("id")
        if not sib_full:
            continue
        sib_id = get_parsed_openalex_id(sib_full)               # e.g. "T11383"

        # 2) If that sibling is already in our graph, get its URI; otherwise, create it now
        sib_uri = global_topic_cache.get(sib_id)

        #only connect them if both topic nodes exist in our graph
        if sib_uri is None:
            continue

        # 3) Add the “relatedToTopic” triple both ways (optional if you want symmetry)
        if (topic_uri, ONTO.relatedToTopic, sib_uri) not in g:
            g.add((topic_uri, ONTO.relatedToTopic, sib_uri))
            
        if (sib_uri, ONTO.relatedToTopic, topic_uri) not in g:
            g.add((sib_uri, ONTO.relatedToTopic, topic_uri))


# In[31]:


output_path = "social_kg.ttl"
g.serialize(destination=output_path, format="turtle")
print(f"✔ Wrote {len(g)} triples to {output_path}")


# In[ ]:




