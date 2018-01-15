""" tasks.py - Defines the task functions of the pipeline for building a NER datset """

__author__ = "Daniel Specht Menezes"
__copyright__ = "Copyright 2018, Daniel Specht Silva Menezes"
__credits__ = ["Daniel Specht Menezes"]
__license__ = "Apache License 2.0"
__version__ = "1.0"
__maintainer__ = "Daniel Specht Menezes"
__email__ = "danielssmenezes@gmail.com"
__status__ = "Development"

from os.path import basename, splitext
import wikipedia
import csv
import re

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
        - Column 'names' - A JSON list containing the column names
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
                names_col:list(filter(None,[j for i in [v.split(separator) for v in row.values()] for j in i]))}
            CSV_outputs.writerow(output_row)

# TODO - Use the ruffus logger to log the exceptions on getting the page
# TODO - Pass the function 'get_wikipedia_page' to the function so the library used for fetching the page can be changed
# TODO - Process the exception lines again
# TODO - Split the files for paralel tasks
# TODO - Save the rows which raised exceptions on another file
def get_wikipedia_page (input_file, output_file):
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

    # TODO - Check the specifications on the wikipage format for section titles
    def get_wikipedia_page(wikiPageId):
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

    # Added output column
    page_col = "page"

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
                    page_col:get_wikipedia_page(row[id_col])} # gets Wikipedia page
                CSV_outputs.writerow(output_row)
            except:
                pass

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

def match_entities (input_file, output_file):
    """
    Recieves a CSV file with:
        - Column 'isPrimaryTopicOf' - The URL of the wiki page
        - Column 'wikiPageID' - The wikipedia page id
        - Column 'names' - A JSON list containing the column names

    Writes a CSV file with:
        - The 'URL','id' and 'names' columns in the input file
        - Column 'page' - A JSON dict containing the keys 'text' and 'links'
        containing the text and links of the Wikipedia page
        If an exception was raised while getting the page, returns the exception name
    """

    pass

# artigo original - https://arxiv.org/pdf/cmp-lg/9505040.pdf
def IOB (input_file, output_file):
    """

    """
    pass

# TODO - Verify if it is best to tag the entities before or after postagging+splitting
# Question - Faz diferença realizar o postagging sobre uma frase extraída?

def sentence_splitting (input_file, output_file):
    """
    """
    pass