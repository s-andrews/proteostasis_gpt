#!/usr/bin/env python3
import math
import xml.etree.ElementTree
import requests
import xmltodict
import xml.etree
import urllib.parse
import tarfile
import tempfile

def main():

    with open("proteostasis_gtp_text.tx","wt", encoding="utf8") as out:
        text = get_document_text("PMC10609202")
        print(text, file=out)
        return


    pubmed_ids = get_pubmed_ids()
    print("Got",len(pubmed_ids),"pubmed hits")
    pmc_ids = get_pmc_ids(pubmed_ids)
    print("Got",len(pmc_ids),"PMC hits")
    for id in pmc_ids:
        get_document_text(id)

def get_document_text(id):
    url = get_tgz_url(id)
    file = download_tgz(url)
    text = read_text_from_tgz(file)

    return text



def read_text_from_tgz(file):
    paragraphs = []

    with tarfile.open(file,"r") as tar:
        for member in tar.getmembers():
            if member.name.endswith(".nxml"):
                fh = tar.extractfile(member)
                # The xmltodict doesn't parse this correctly.  We'll need to do something
                # which goes through the tree
                root = xml.etree.ElementTree.parse(fh).getroot()

                # Find the body
                body = None
                for child in root:
                    if child.tag == "body":
                        body = child
                        break


                for section in body:
                    
                    for part in section:
                        if part.tag == "title":
                            # We only want introduction, results and discussion
                            if not ("intro" in part.text.lower() or "result" in part.text.lower() or "discus" in part.text.lower()):
                                # We're not parsing this stuff
                                break

                            paragraphs.append(part.text)


                        else:
                            # This is a paragraph
                            paragraphs.append(part.text)

                            # We now need to add anything beyond the embedded tags in the paragraph
                            for subsequent_tag in part:
                                paragraphs[-1] += subsequent_tag.tail


    # TODO
    # We'll have some empty reference text which looks like [] or [,,,,] which we can remove

    return "\n\n".join(paragraphs)


def download_tgz(url):
    with requests.get(url, stream=True) as infh:
        temp = tempfile.NamedTemporaryFile("wb",suffix=".tar.gz", delete=False)
        infh.raise_for_status()
        for chunk in infh.iter_content(chunk_size=8192):
            temp.write(chunk)


    temp.close()

    return(temp.name)


def get_pmc_ids(pubmed_ids):
    # We convert from pubmed to PMC.  Not all pubmed ids will be in
    # PMC so we're expecting some losses here.

    # The API we're using says it can do up to 200 ids in a batch.
    # We'll stick to 100 to be safe.

    api_url = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?tool=proteostasisGPT&email=simon.andrews@babraham.ac.uk&ids="

    pmc_ids = []

    for startpos in range(0,math.ceil(len(pubmed_ids)/100)):
        startpos = startpos * 100
        endpos = startpos + 100
        if endpos > len(pubmed_ids):
            endpos = len(pubmed_ids)
        print("Starting at",startpos,"ending at",endpos)
        this_batch = pubmed_ids[startpos:endpos]

        this_url = api_url+",".join(this_batch)

        response = requests.get(this_url)
        xml_data = xmltodict.parse(response.content)

        for record in xml_data["pmcids"]["record"]:
            if "@pmcid" in record:
                pmc_ids.append(record["@pmcid"])

    return pmc_ids


def get_pubmed_ids():
    pubmed_url = "https://pubmed.ncbi.nlm.nih.gov/?format=pmid&sort=date&size=100&term="+urllib.parse.quote_plus("(proteostasis[Title]) AND (\"2021/01/01\"[Publication Date] : \"3000\"[Publication Date])")


    pubmed_ids = []

    page = 0

    while True:
        page += 1
        print("Trying page",page)
        new_ids = []
        response = requests.get(pubmed_url+f"&page={page}")


        for line in response.content.decode("utf8").split("\n"):
            line = line.strip()
            if not line:
                continue
            pmid = line.split()[0].strip()
            if pmid.isnumeric():
                new_ids.append(pmid)

        if not new_ids:
            break

        pubmed_ids.extend(new_ids)


    return pubmed_ids



def get_tgz_url(id):
    url = "https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?id="+id

    response = requests.get(url)

    xml_doc = xmltodict.parse(response.content)

    if not "records" in xml_doc["OA"]:
        print("Skipped",id)
        return None

    record = xml_doc["OA"]["records"]["record"]

    link = record["link"]
    if link["@format"] != "tgz":
        print("Non tgz link for",id)
        return None

    tgz_url = link["@href"].replace("ftp://","https://")

    return tgz_url
     

if __name__ == "__main__":
    main()