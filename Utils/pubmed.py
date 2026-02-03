import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import os
import tarfile

# Base URL for NCBI E-utilities
BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"

def search_pmc(term, max_results=5, mindate=None, maxdate=None):
    """
    Searches PubMed Central for a term and returns a list of PMC IDs.
    If no date range is provided, defaults to the last 15 days.
    Dates should be in YYYY/MM/DD format.
    """
    url = f"{BASE_URL}esearch.fcgi"
    
    # Default to last 15 days if no dates provided
    if mindate is None and maxdate is None:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=15)
        mindate = start_date.strftime("%Y/%m/%d")
        maxdate = end_date.strftime("%Y/%m/%d")

    params = {
        "db": "pmc",
        "term": term,
        "retmode": "json",
        "retmax": max_results,
        "datetype": "pdat",  # Publication date
        "mindate": mindate,
        "maxdate": maxdate
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    try:
        id_list = data["esearchresult"]["idlist"]
        print(f"Searching from {mindate} to {maxdate}...")
        return id_list
    except KeyError:
        return []
    

def get_pmc_metadata(id_list):
    """
    Takes a list of PMC IDs and fetches metadata including abstract, keywords, and references.
    Uses efetch (XML) as esummary (JSON) does not provide this depth. 
    """
    if not id_list:
        return []

    url = f"{BASE_URL}efetch.fcgi"
    ids_string = ",".join(id_list)
    
    params = {
        "db": "pmc",
        "id": ids_string,
        "retmode": "xml"
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    # Parse XML response
    try:
        root = ET.fromstring(response.content)
    except ET.ParseError:
        print("Failed to parse XML response")
        return []

    articles = []
    
    for article in root.findall(".//article"):
        # Basic Metadata
        meta = article.find(".//article-meta")
        if meta is None:
            continue
            
        # Title
        title_node = meta.find(".//article-title")
        title = "".join(title_node.itertext()) if title_node is not None else "No Title"
        
        # ID (PMC)
        pmcid_node = meta.find(".//article-id[@pub-id-type='pmcid']")
        if pmcid_node is not None:
             # Some XMLs have "PMC123" others just "123". Ensure one "PMC" prefix.
            pmcid_text = pmcid_node.text
            if pmcid_text.startswith("PMC"):
                pmcid = pmcid_text
            else:
                pmcid = f"PMC{pmcid_text}"
        else:
            pmcid = "Unknown"

        # Abstract
        abstract_node = meta.find(".//abstract")
        abstract = "".join(abstract_node.itertext()).strip() if abstract_node is not None else "No Abstract"
        
        # Keywords (Proxy for MeSH)
        keywords = []
        for kw in meta.findall(".//kwd"):
            if kw.text:
                keywords.append(kw.text)
        
        # Publication Type
        pub_type = article.get("article-type", "Unknown")

        # References
        refs = []
        ref_list = article.findall(".//ref")
        for ref in ref_list:
            # Try to get mixed-citation or citation
            citation = ref.find(".//mixed-citation") or ref.find(".//citation") or ref.find(".//element-citation")
            if citation is not None:
                refs.append("".join(citation.itertext()).strip())
        
        # Journal Info
        journal_node = article.find(".//journal-title")
        journal = journal_node.text if journal_node is not None else "Unknown Journal"

        # Pub Date
        pub_date_node = article.find(".//pub-date")
        if pub_date_node is not None:
            year = pub_date_node.find("year")
            month = pub_date_node.find("month")
            day = pub_date_node.find("day")
            pub_date = f"{year.text if year is not None else ''}-{month.text if month is not None else '01'}-{day.text if day is not None else '01'}"
        else:
            pub_date = "Unknown Date"

        # Authors
        authors = []
        for contrib in meta.findall(".//contrib[@contrib-type='author']"):
            surname = contrib.find(".//surname")
            given_names = contrib.find(".//given-names")
            if surname is not None and given_names is not None:
                authors.append(f"{surname.text}, {given_names.text}")
            elif surname is not None:
                authors.append(surname.text)

        articles.append({
            "pmcid": pmcid,
            "title": title,
            "journal": journal,
            "pub_date": pub_date,
            "authors": authors,
            "pub_type": pub_type,
            "abstract": abstract,
            "mesh_terms": keywords, 
            "references": refs
        })
        
    return articles


def download_article_files(pmcid, save_dir="downloads"):
    """
    Uses the PMC Open Access Web Service to download PDF and XML files.
    Renames them to {pmcid}.pdf and {pmcid}.nxml.
    """
    oa_url = "https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi"
    params = {"id": pmcid}
    
    try:
        response = requests.get(oa_url, params=params)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error querying OA API for {pmcid}: {e}")
        return

    try:
        root = ET.fromstring(response.content)
    except ET.ParseError:
        print(f"Failed to parse OA XML for {pmcid}")
        return

    error = root.find(".//error")
    if error is not None:
        print(f"OA API Error for {pmcid}: {error.get('code')} - {error.text}")
        return

    article_dir = os.path.join(save_dir, pmcid)
    os.makedirs(article_dir, exist_ok=True)

    links = {}
    for link in root.findall(".//link"):
        fmt = link.get("format")
        href = link.get("href")
        if href:
            if href.startswith("ftp://"):
                href = href.replace("ftp://", "https://")
            links[fmt] = href

    downloaded_something = False

    # 1. Try Direct PDF/XML
    if 'pdf' in links or 'xml' in links:
        for fmt, ext in [('pdf', 'pdf'), ('xml', 'nxml')]:
            if fmt in links:
                href = links[fmt]
                save_name = f"{pmcid}.{ext}"
                save_path = os.path.join(article_dir, save_name)
                
                print(f"Downloading {fmt.upper()} as {save_name}...")
                try:
                    with requests.get(href, stream=True) as r:
                        r.raise_for_status()
                        with open(save_path, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=8192):
                                f.write(chunk)
                    print(f"Saved {save_name}")
                    downloaded_something = True
                except Exception as e:
                    print(f"Failed to download {href}: {e}")

    # 2. Fallback to TGZ
    if not downloaded_something and 'tgz' in links:
        href = links['tgz']
        tgz_name = os.path.basename(href)
        tgz_path = os.path.join(article_dir, tgz_name)
        
        print(f"Downloading OA Package (TGZ): {tgz_name}...")
        try:
            with requests.get(href, stream=True) as r:
                r.raise_for_status()
                with open(tgz_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            print(f"Extracting {tgz_name}...")
            try:
                with tarfile.open(tgz_path, "r:gz") as tar:
                    tar.extractall(path=article_dir)
                
                os.remove(tgz_path) 
                downloaded_something = True
                
                # Rename extracted files to standard format
                for root_path, dirs, files in os.walk(article_dir):
                    for file in files:
                        if file.lower().endswith(".pdf"):
                             old_path = os.path.join(root_path, file)
                             new_path = os.path.join(article_dir, f"{pmcid}.pdf")
                             # Avoid overwriting if multiple PDFs exist (unlikely but possible)
                             if not os.path.exists(new_path):
                                 os.rename(old_path, new_path)
                                 print(f"Renamed {file} to {pmcid}.pdf")

                        elif file.lower().endswith(".nxml") or file.lower().endswith(".xml"):
                             old_path = os.path.join(root_path, file)
                             new_path = os.path.join(article_dir, f"{pmcid}.nxml")
                             if not os.path.exists(new_path):
                                 os.rename(old_path, new_path)
                                 print(f"Renamed {file} to {pmcid}.nxml")
                
            except tarfile.TarError as e:
                print(f"Failed to extract tarball: {e}")
                
        except Exception as e:
            print(f"Failed to download package {href}: {e}")

    if not downloaded_something:
        print(f"No accessible files found for {pmcid} (might not be Open Access).")

