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
#import nltk
import json
import csv
import sys
import re

csv.field_size_limit(sys.maxsize)

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
            names = list(set(filter(None,[j for i in [v.split(separator) for v in row.values()] for j in i])))
            names = json.dumps(names)

            output_row = {
                id_col:row.pop(id_col), # keeps id_col
                url_col:row.pop(url_col), # keeps id_col
                # Merge other cell values removing empty string and splitting on given separator
                names_col:names}
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
        - The required columns for the the input file, except 'page'
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
        True if the paragraph is wikitext for displaying an image trough the "thumb" keyword
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
            
            sentences = json.dumps(_split_article_sentences(row[plain_text_col]))

            output_row = {
                id_col:row[id_col],
                url_col:row[url_col],
                names_col: row[names_col],
                sentences_col: sentences}

            CSV_outputs.writerow(output_row)

def filter_sentences_by_mentions(sentences,names):
    """
    Recieves a list of sentences and a list of names.
    Returns the sentences in which at leas one of the mentioned names appear.
    """
    # filter(lambda x: any(name in sentence for name in names),sentences)
    
    sents = []
    for sentence in sentences:
        if any(name in sentence for name in names):
            sents.append(sentence)
    return sents

def filter_sentences_with_entities (input_file, output_file):
    """
    Recieves a CSV file with:
        - Column 'isPrimaryTopicOf' - The URL of the wiki page
        - Column 'wikiPageID' - The wikipedia page id
        - Column 'names' - A JSON list containing the column names
        - Column 'sentences' - A list with the extracted sentences from the recieved 'plainText'

    Writes a CSV file with:
        - The required recieved columns.
        - The sentences in the column "sentence" that don't mention any of the names of the column
        "names" will be removed       
    """
    # Recieved columns
    id_col = "wikiPageID"
    url_col = "isPrimaryTopicOf"
    names_col = "names"
    sentences_col = "sentences"

    output_columns = [id_col,url_col,names_col,sentences_col]

    with open(input_file, 'r') as inputs, open(output_file, 'w') as outputs:

        CSV_inputs = csv.DictReader(inputs)
        CSV_outputs = csv.DictWriter(outputs, fieldnames=output_columns)
        CSV_outputs.writeheader()

        for row in CSV_inputs:
            sentences = json.loads(row[sentences_col])
            names = json.loads(row[names_col])

            output_row = {
                id_col:row[id_col],
                url_col:row[url_col],
                names_col: row[names_col],
                sentences_col:json.dumps(filter_sentences_by_mentions(sentences,names))}
            CSV_outputs.writerow(output_row)

def split_words (sentence):
    """
    Returns the words of the recieved sentence
    """
    tokens =[]
    token = []
    for i in range(len(sentence)):
        c = sentence[i]
        
        if c == "," or c == ":":
            # case: "Today, allright?"
            if i+1 == len(sentence):
                if token:
                    tokens.append("".join(token))
                    token =[]
                tokens.append(c)

            elif i+1 <= len(sentence) and sentence[i+1] == " ":
                if token:
                    tokens.append("".join(token))
                    token =[]
                tokens.append(c)
            else:
                token.append(c)
    
        # 3 pontos?
        elif c == "." or c == "?" or c == "!" :
            # last token or ... or ..?!
            if i == len(sentence)-1 or sentence[i+1] == "." or sentence[i+1] == "?" or sentence[i+1] == "!" :
                if token:
                    tokens.append("".join(token))
                    token =[]

                tokens.append(c)
            else:
                token.append(c)
        
        elif c == " ":
            if token:
                tokens.append("".join(token))
                token =[]
        else:
            token.append(c)

    if token:
        tokens.append("".join(token))

    return tokens

#print(split_words("oi gente, olha, aa,a.aaa isso.!.?"))
    
    #return nltk.tokenize.word_tokenize(sentence,language='portuguese')

    #words = nltk.word_tokenize


def split_sentences_entities (input_file, output_file,word_splitter):
    """
     - word_splitter - A function for splitting a sentence into words

    Recieves a CSV file with:
        - Column 'isPrimaryTopicOf' - The URL of the wiki page
        - Column 'wikiPageID' - The wikipedia page id
        - Column 'names' - A JSON list containing the column names
        - Column 'sentences' column - a list with the extracted sentences from the article

    Writes a CSV file with:
        - Added 'tokenizedSentences' column - A json list of the tokens of the sentence
        - Added 'tokenizedNames' column - A json list of the tokens of the names
    """

    # Recieved columns
    id_col = "wikiPageID"
    url_col = "isPrimaryTopicOf"
    names_col = "names"
    sentences_col = "sentences"

    # Added columns
    tokenized_sentences_col = "tokenizedSentences"
    tokenized_names_col = "tokenizedNames"

    output_columns = [id_col,url_col,names_col,sentences_col,tokenized_names_col,tokenized_sentences_col]

    with open(input_file, 'r') as inputs, open(output_file, 'w') as outputs:

        CSV_inputs = csv.DictReader(inputs)
        CSV_outputs = csv.DictWriter(outputs, fieldnames=output_columns)
        CSV_outputs.writeheader()

        for row in CSV_inputs:
            sentences = json.loads(row[sentences_col])
            names = json.loads(row[names_col])

            tokenized_sentences = json.dumps([split_words(sentence) for sentence in sentences])
            tokenized_names = json.dumps([split_words(name) for name in names])

            output_row = {
                id_col:row[id_col],
                url_col:row[url_col],
                names_col: row[names_col],
                sentences_col: row[sentences_col],
                tokenized_names_col: tokenized_names,
                tokenized_sentences_col: tokenized_sentences}

            CSV_outputs.writerow(output_row)

def score_counter(sentence_tokens,entity_tokens,sentence_index=0,entity_index=0,current_score=0,exact_matching=True):
    """
    Recieved
        - sentence_tokens - The tokens of the sentence
        - entity_tokens - The tokens of the entity
        - sentence_index - The current index of the sentence
        - entity_index - The current index of the entity
        - current_score=0 - The current score of the entity

    Returns the score of the entity.
    The score is an integer that indicates how many tokens of the entity in order are present in the
    sentence starting from the recieved sentence_index.
    """

    def counter_aux(sentence_tokens,entity_tokens,sentence_index,entity_index,current_score):
        """
        Auxiliary recursive function for the 'score_counter' function.
        Does not check for exact matching.
        """
       
        if sentence_tokens[sentence_index] == entity_tokens[entity_index]:
            # Reached last token of the sentence or entity
            if sentence_index+1 == len(sentence_tokens) or entity_index+1 == len(entity_tokens):
                return current_score+1
            return counter_aux(sentence_tokens, entity_tokens, sentence_index+1, entity_index+1, current_score+1)
        return current_score

    score = counter_aux(sentence_tokens,entity_tokens,sentence_index,entity_index,current_score)
    # If exact matching selected, ignore other cases
    if exact_matching and not score == len(entity_tokens):
        return 0
    return score

def match_entities(tokenized_entities, tokenized_sentence, exact_matching=True):
    """
    Recieves:
        tokenized_entities - List of the tokenized entities
        tokenized_sentence - The sentence tokens
        exact_matching - Flag that indicates if only exact matchings of the entities will be considered.
            An exact matching occurs when the full text of the entity name is present

    Returns:
        List [[(init,end) ...] ...] in which:
            - 'init','end' - The segment of occurence of the entity
            - The matches of the specific entity are in the id of returned list which is the same as its id in the recieved list
    """
    matches = [[] for i in range(len(tokenized_entities))]
    i=0
    while i < len(tokenized_sentence):
        entity_scores = [score_counter(tokenized_sentence, entity_tokens, i, exact_matching=exact_matching) for entity_tokens in tokenized_entities]
        entity_score,entity_id = max([(v,i) for i,v in enumerate(entity_scores)])
        
        # Entity matches
        if entity_score != 0:
            a = [i,entity_score-1]
            
            matches[entity_id].append(a)
        i = i + max([entity_score,1])

    return matches

#tokenized_entities = [["token1.1","token1.2"],["token2.1","token2.2"],["token3.1","token3.2","token3.3"]]
#tokenized_sentence = ["token1.1","token1.2", "olha1","token1.1","token1.2","olha2","token3.1","token3.2","token3.3","olha3","token2.1","token2.2","olha4","token2.1"]
#matches = match_entities(tokenized_entities,tokenized_sentence,True)
#print(matches)

def annotate_sentences_entities (input_file, output_file):
    """
    Recieves a CSV file with:
        - Column 'isPrimaryTopicOf' - The URL of the wiki page
        - Column 'wikiPageID' - The wikipedia page id
        - Column 'names' - A JSON list containing the column names
        - Column 'sentences' - a list with the extracted sentences from the article
        - Column 'tokenizedSentences' - A json list of the tokens of the sentence
        - Column 'tokenizedNames' - A json list of the tokens of the names

    Writes a CSV file with:
        - Added 'annotatedEntities' - a structure in the format [ [(init,end),(init,end) ...] [(init,end),(init,end) ...] ...]
        The first element of the list is a list corresponding to the occurences of the name of index of same index in the column 'sentences'
        The 'init' and 'end' are the beginning and end of the name in the tokens of the sentence in the column 'tokenizedSentence'
    """

    # Recieved columns
    id_col = "wikiPageID"
    url_col = "isPrimaryTopicOf"
    names_col = "names"
    sentences_col = "sentences"
    tokenized_sentences_col = "tokenizedSentences"
    tokenized_names_col = "tokenizedNames"

    # Added columns
    annotated_entities_col = "annotatedEntities"

    output_columns = [id_col,url_col,names_col,sentences_col,tokenized_sentences_col,tokenized_names_col,annotated_entities_col]

    with open(input_file, 'r') as inputs, open(output_file, 'w') as outputs:

        CSV_inputs = csv.DictReader(inputs)
        CSV_outputs = csv.DictWriter(outputs, fieldnames=output_columns)
        CSV_outputs.writeheader()

        for row in CSV_inputs:
            tokenized_sentences = json.loads(row[tokenized_sentences_col])
            tokenized_names = json.loads(row[tokenized_names_col])

            annotated_entities = [match_entities(tokenized_names,tokenized_sentence) for tokenized_sentence in tokenized_sentences]

            output_row = {
                id_col: row[id_col],
                url_col: row[url_col],
                names_col: row[names_col],
                sentences_col: row[sentences_col],
                tokenized_sentences_col: row[tokenized_sentences_col],
                tokenized_names_col: row[tokenized_names_col],
                annotated_entities_col: annotated_entities}

            CSV_outputs.writerow(output_row)

# Artigo original - https://arxiv.org/pdf/cmp-lg/9505040.pdf
def IOB (input_file, output_file):
    """
    Recieves a CSV file with:
        - Column 'isPrimaryTopicOf' - The URL of the wiki page
        - Column 'wikiPageID' - The wikipedia page id
        - Column 'names' - A JSON list containing the column names
        - Column 'sentences' - A list with the extracted sentences from the article
        - Column 'tokenizedSentences' - A json list of the tokens of the sentence
        - Column 'tokenizedNames' - A json list of the tokens of the names
        - Column 'annotatedEntities' - A structure in the format [ [(init,end),(init,end) ...] [(init,end),(init,end) ...] ...]
        The first element of the list is a list corresponding to the occurences of the name of index of same index in the column 'sentences'
        The 'init' and 'end' are the beginning and end of the name in the tokens of the sentence in the column 'tokenizedSentence'

    Writes a .conll file in the IOB format
    """

    # Recieved columns
    id_col = "wikiPageID"
    url_col = "isPrimaryTopicOf"
    names_col = "names"
    sentences_col = "sentences"
    tokenized_sentences_col = "tokenizedSentences"
    tokenized_names_col = "tokenizedNames"
    annotated_entities_col = "annotatedEntities"

    with open(input_file, 'r') as inputs, open(output_file, 'w') as outputs:

        CSV_inputs = csv.DictReader(inputs)

        inside_flag = "I"
        outside_flag = "O"
        begin_flag = "B"

        org_flag = "ORG"
        location_flag = "LOC"
        person_flag = "PER"

        for row in CSV_inputs:
            sentence_tokens = json.loads(row[tokenized_sentences_col])
            annotated_entities = json.loads(row[annotated_entities_col])

            sentences = json.loads(row[sentences_col])
            names = json.loads(row[names_col])
            
            for sentence_index, sentence_matches in enumerate(annotated_entities):
                corresponding_sentence = sentences[sentence_index]

                lines = [{"token":token,"position":outside_flag,"class":""} for token in sentence_tokens[sentence_index] ]
            
                for entity_index, entity_matches in enumerate(sentence_matches):
                    corresponding_entity = names[entity_index]

                    for match in entity_matches:
                        
                        # TODO: Change to inform the entity type elsewhere
                        # i.e. entity_type = entity["type"]
                        type_flag = ""
                        if "Person" in input_file:
                            type_flag = person_flag
                        elif "Organisation" in input_file:
                            type_flag = org_flag
                        elif "Place" in input_file:
                            type_flag = location_flag

                        # print(annotated_entities)
                        # print(matches)
                        # print(match)
                        init = match[0]
                        end = match[1]

                        lines[init]["position"] = begin_flag
                        lines[init]["class"] = type_flag

                        for i in range(init+1,end+1):
                            lines[init]["position"] = inside_flag
                            lines[init]["class"] = type_flag

                outputs.write(row[names_col])
                for line in lines:
                    if line["position"] == outside_flag:
                        outputs.write("%s\t%s\n"%(line["token"],line["position"]))
                    else:
                        outputs.write("%s\t%s-%s\n"%(line["token"],line["position"],line["class"]))
                outputs.write("\n")

def apply_postaggers (sentences,postaggers):
    return [{"sentence":sentence,"annotations": {postagger_name:postagger_function(sentence) for postagger_name, postagger_function in postaggers.items()}} for sentence in sentences]

def annotate_sentences_with_postaggers (input_file, output_file, postaggers):
    """
    Recieves a CSV file with:
        - Column 'isPrimaryTopicOf' - The URL of the wiki page
        - Column 'wikiPageID' - The wikipedia page id
        - Column 'names' - A JSON list containing the column names
        - Column 'sentences' column - a list with the extracted sentences from the article

    Writes a CSV file with:
        - The required recieved columns except "sentences"
        - Added "annottations" column - A empty json with dict : {}
    """

    # Recieved columns
    id_col = "wikiPageID"
    url_col = "isPrimaryTopicOf"
    names_col = "names"
    sentences_col = "sentences"

    # Added columns
    annotated_col = "annotated"
    output_columns = [id_col,url_col,names_col,annotated_col]

    with open(input_file, 'r') as inputs, open(output_file, 'w') as outputs:

        CSV_inputs = csv.DictReader(inputs)
        CSV_outputs = csv.DictWriter(outputs, fieldnames=output_columns)
        CSV_outputs.writeheader()

        for row in CSV_inputs:
            sentences = json.loads(row[sentences_col])

            output_row = {
                id_col:row[id_col],
                url_col:row[url_col],
                names_col: row[names_col],
                annotated_col: apply_postaggers(sentences,postaggers)}
            CSV_outputs.writerow(output_row)

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
