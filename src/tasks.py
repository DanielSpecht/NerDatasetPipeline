""" tasks.py - Defines the task functions of the pipeline for building a NER datset """

__author__ = "Daniel Specht Menezes"
__copyright__ = "Copyright 2018, Daniel Specht Silva Menezes"
__credits__ = ["Daniel Specht Menezes"]
__license__ = "Apache License 2.0"
__version__ = "1.0"
__maintainer__ = "Daniel Specht Menezes"
__email__ = "danielssmenezes@gmail.com"
__status__ = "Development"

from os.path import basename, splitext, getsize
import mwparserfromhell
import wikipedia
import sqlite3
import json
import csv
import sys
import re

csv.field_size_limit(sys.maxsize)

# TODO - Change the name of the columns WikiPageURL, wikiPageID so it it capitalized correctly
# TODO - Colocar no arquivo importado o tipo da entidade nas linhas, não só no nome
# TODO (All stages) - Passar como parâmetro os nomes das chaves que não são nomes de entidade e o separador
def summarize_entity_names(input_file,output_file):
    """ 
    Recieves a csv file with:
        - Column 'isPrimaryTopicOf' - The URL of the wiki page
        - Column 'wikiPageID' - The wikipedia page id
        - Other columns are possible entity names.
          Many names may exist in the same column separated by ';;'

    Writes a CSV file with:
        - The recieved WikiPageURL and wikiPageID columns
        - Column 'names' - A JSON list containing the column names. No duplicate names.
    """

    separator = ";;"

    # Recieved, unchanged columns
    id_col = "wikiPageID"
    url_col = "isPrimaryTopicOf"

    # Added output column
    names_col = "names"

    output_columns = [id_col,url_col,names_col]

    with open(input_file, 'r') as inputs, open(output_file, 'w') as outputs:

        CSV_inputs = csv.DictReader(inputs)
        CSV_outputs = csv.DictWriter(outputs, fieldnames=output_columns)
        CSV_outputs.writeheader()

        for row in CSV_inputs: 
            output_row = {
                id_col:row.pop(id_col), # keeps id_col
                url_col:row.pop(url_col), # keeps id_col
                # Merge other cell values removing empty string and splitting on given separator
                names_col:list(set(filter(None,[j for i in [v.split(separator) for v in row.values()] for j in i])))}
            CSV_outputs.writerow(output_row)

def _get_article_info(article_id):
    """
    Recieves the wikipedia article id
    Returns a json string containing a dict with the keys:
     - 'text' - The wikitext of the article
     - 'title' - The title of the article

    If the article is not found ot the text is empty, returns none
    """

    db = '/home/daniel/Documents/wikipedia dump/wikipedia2016.db'
    conn = sqlite3.connect(db)
    query = """
            SELECT title, content
            FROM WikiElement
            WHERE id == %s
            """%(article_id)
    #print(query)
    result = conn.execute(query).fetchall()

    if not len(result) == 1:
        return None

    article = result[0]

    if not article[1]:
        return None
    
    return json.dumps({"title":article[0].replace("''","'"),"text":article[1].replace("''","'") })

def get_wikipedia_page(input_file,output_file,discarded_file="./discarded.csv"):
    """
    Recieves a CSV file with:
        - Column 'isPrimaryTopicOf' - The URL of the wiki page
        - Column 'wikiPageID' - The wikipedia page id
        - Column 'names' - A JSON list containing the column names

    Writes a CSV file with:
        - The required columns for the the input file, discarding the others
        - Added column 'page' - A JSON dict containing the keys 'text' and 'title'
        The values of these kays may be an empty string

        If the id of the wikipedia page yields more than one result, the line is discarded and logged into a file
        'discarded_rows.csv'
    """

    # Recieved, unchanged columns
    id_col = "wikiPageID"
    url_col = "isPrimaryTopicOf"
    names_col = "names"

    # Added output column
    page_col = "page"

    output_columns = [id_col,url_col,names_col,page_col]

    with open(input_file, 'r') as inputs, open(output_file, 'w') as outputs, open(discarded_file, 'a') as discarded:

        CSV_inputs = csv.DictReader(inputs)

        CSV_outputs = csv.DictWriter(outputs, fieldnames=output_columns)
        CSV_outputs.writeheader()

        CSV_discarded = csv.DictWriter(discarded, fieldnames=CSV_inputs.fieldnames)
        if getsize(discarded_file) == 0:
            CSV_discarded.writeheader()

        for row in CSV_inputs:
            article_info = _get_article_info(row[id_col])
            if article_info:
                output_row = {
                    id_col:row[id_col], # keeps id_col
                    url_col:row[url_col],
                    names_col:row[names_col],
                    # Merge other cell values removing empty string and splitting on given separator
                    page_col:article_info}
                CSV_outputs.writerow(output_row)
            else:
                CSV_discarded.writerow(row)

def get_wikipedia_plain_text(input_file, output_file):
    """
    Recieves a CSV file with:
        - Column 'isPrimaryTopicOf' - The URL of the wiki page
        - Column 'wikiPageID' - The wikipedia page id
        - Column 'names' - A JSON list containing the column names
        - Column 'page' - A JSON dict containing the keys 'text' and 'title'

    Writes a CSV file with:
        - The required columns for the the input file, except 'páge'
        - Added column 'plainText' - A string containig the plain text of the article
    """

    # Recieved columns
    id_col = "wikiPageID"
    url_col = "isPrimaryTopicOf"
    names_col = "names"
    page_col = "page"

    # Added output column
    plain_text_col = "plainText"

    output_columns = [id_col,url_col,names_col,plain_text_col]

    with open(input_file, 'r') as inputs, open(output_file, 'w') as outputs:

        CSV_inputs = csv.DictReader(inputs)
        CSV_outputs = csv.DictWriter(outputs, fieldnames=output_columns)
        CSV_outputs.writeheader()

        for row in CSV_inputs:
            wiki_text = json.loads(row["page"])["text"]
            plain_text = mwparserfromhell.parse(wiki_text).strip_code()

            output_row = {
                id_col:row[id_col],
                url_col:row[url_col],
                names_col: row[names_col],
                plain_text_col: plain_text}

            CSV_outputs.writerow(output_row)

def _split_article_sentences(article_text):
    """
    Recieves a string containing the plain text of a wikipedia article
    Returns a list containing the sentences of the recieved article text
    """

    def not_image_thumb(paragraph):
        """
        True if the paragraph is wikitext for displaying an image trough the tumb keyword
        i.e. thumb|25px|left
        """
        return not bool(re.match("\|thumb\|?|\|?thumb\|",paragraph))

    def split_sentences(text):
        """
        Returns a list of the sentences in the text
        """
        sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?|\!)\s', text)
        return sentences

    paragraphs = article_text.split("\n")

    # Remove empty paragraphs, composed by 0 or more space characters
    paragraphs = list(filter(lambda x: not bool(re.match(r'^\s*$', x)) , paragraphs))
    
    # Remove the paragraphs corresponding to an image thumb
    paragraphs = list(filter(not_image_thumb, paragraphs))
    
    # Split the paragraphs into sentences
    # i.e [pragraph1,paragraph] -> [[p1_sent1,p1_sent2],[p2_sent1]]
    sentences = list(map(split_sentences,paragraphs))
    
    # Single list i.e. [[1,2],[3]] -> [1,2,3]
    sentences = [j for i in sentences for j in i]
    
    # Remove sentences not ending in '.!?'
    sentences = list(filter(lambda x: bool(re.match(r'.+[\?|\!|\.]$', x)), sentences))

    return sentences

def sentence_splitting (input_file, output_file):
    """
    Recieves a CSV file with:
        - Column 'isPrimaryTopicOf' - The URL of the wiki page
        - Column 'wikiPageID' - The wikipedia page id
        - Column 'names' - A JSON list containing the column names
        - Column 'plainText' - A string containig the plain text of the article

    Writes a CSV file with:
        - The required columns for the the input file, except 'plainText'
        - Added 'sentences' column - a list with the extracted sentences from the recieved 'plainText'
    """
    # Recieved columns
    id_col = "wikiPageID"
    url_col = "isPrimaryTopicOf"
    names_col = "names"
    plain_text_col = "plainText"

    # Added output column
    sentences_col = "sentences"

    output_columns = [id_col,url_col,names_col,sentences_col]

    with open(input_file, 'r') as inputs, open(output_file, 'w') as outputs:

        CSV_inputs = csv.DictReader(inputs)
        CSV_outputs = csv.DictWriter(outputs, fieldnames=output_columns)
        CSV_outputs.writeheader()

        for row in CSV_inputs:
            
            sentences = _split_article_sentences(row[plain_text_col])

            output_row = {
                id_col:row[id_col],
                url_col:row[url_col],
                names_col: row[names_col],
                sentences_col: sentences}

            CSV_outputs.writerow(output_row)

def match_entities (input_file, output_file):
    """
    """
    pass

# artigo original - https://arxiv.org/pdf/cmp-lg/9505040.pdf
def IOB (input_file, output_file):
    """
    """
    pass

# TODO - Verify if it is best to tag the entities before or after postagging+splitting
# Question - Faz diferença realizar o postagging sobre uma frase extraída?

# [DEPRECATED]
def request_wikipedia_pages (input_file, output_file):
    """
    Recieves a CSV file with:
        - Column 'isPrimaryTopicOf' - The URL of the wiki page
        - Column 'wikiPageID' - The wikipedia page id
        - Column 'names' - A JSON list containing the column names

    Writes a CSV file with:
        - The 'URL','id' and 'names' columns in the input file
        - Column 'page' - A JSON dict containing the keys 'text' and 'links'
        containing the text and links of the Wikipedia page
        If an exception was raised while getting the page, the row is discarded
    """

    def request_page (wikiPageId):
        """
        Returns a dict with the keys:
            - 'text' - The wikipedia page plain text
            - 'links' - The referenced Wikipedia links
        """
        wikiPage = wikipedia.page(wikiPageId)
        print(wikiPage.url)
        # Removes the section titiles i.e. "=== Title ==="
        return {"text":re.sub('=+ .* =+','',wikiPage.content,flags=re.MULTILINE),
                "links":wikiPage.links}

    # Recieved, unchanged columns
    id_col = "wikiPageID"
    url_col = "isPrimaryTopicOf"
    names_col = "names"

    # Adipedia.set_rate_limiting(rate_limit=False)
    wikipedia.set_lang("pt")
    with open(input_file, 'r') as inputs, open(output_file, 'w') as outputs:

        CSV_inputs = csv.DictReader(inputs)
        CSV_outputs = csv.DictWriter(outputs, fieldnames=output_columns)
        CSV_outputs.writeheader()

        for row in CSV_inputs:
            try:
                output_row = {
                    id_col:row[id_col], # keeps id_col
                    url_col:row[url_col], # keeps url_col
                    names_col:row[names_col], # keeps name_col
                    page_col:request_page(row[id_col])} # gets Wikipedia page
                CSV_outputs.writerow(output_row)
            except:
                pass
    output_columns = [id_col,url_col,names_col,page_col]

    wikipedia.set_rate_limiting(rate_limit=False)
    wikipedia.set_lang("pt")
    with open(input_file, 'r') as inputs, open(output_file, 'w') as outputs:

        CSV_inputs = csv.DictReader(inputs)
        CSV_outputs = csv.DictWriter(outputs, fieldnames=output_columns)
        CSV_outputs.writeheader()

        for row in CSV_inputs:
            try:
                output_row = {
                    id_col:row[id_col], # keeps id_col
                    url_col:row[url_col], # keeps url_col
                    names_col:row[names_col], # keeps name_col
                    page_col:request_page(row[id_col])} # gets Wikipedia page
                CSV_outputs.writerow(output_row)
            except:
                pass

# [DEPRECATED]
def split_csv_file(file_path,file_name_stem,chunks,extension):
    """
    Splits a file into chunks saving those into files with a specified name format
    The name format is as follows: [file_name_stem].[nth_chunk][extension] 

    Recieves:
        - file_path - The path of the file to be splitted
        - file_name_stem - The basic format from which the new file name must be build upon
        - chunks - The quantity of 'chunk files' in which the sent file must be splitted
        - extension - The file extension of the chunks created
    """

    n_lines = sum(1 for line in csv.DictReader(open(file_path,"r")))
    n_rest = n_lines%chunks
    chunk_size = int(n_lines/chunks)

    with open(file_path,"r") as file:
        csv_file = csv.DictReader(file)
        
        for nth_chunk in range(chunks):
            # [file_name_steam].[nth][extension]
            chunk_file_name = file_name_stem +"."+str(nth_chunk) + extension
            with open(chunk_file_name,"w") as chunk_file:
                writer = csv.DictWriter(chunk_file,csv_file.fieldnames)                
                writer.writeheader()

                for i in range(chunk_size):
                    writer.writerow(csv_file.__next__())

                if n_rest > 0:
                    n_rest-=1
                    writer.writerow(csv_file.__next__())