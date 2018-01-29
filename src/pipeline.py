from ruffus import *
import postaggers
import wikipedia
import tasks
import json
import csv
import os
import re

def getFiles():
    folders = ["/home/daniel/Repositories/ruffusTests/MyTests/Organisation",
           "/home/daniel/Repositories/ruffusTests/MyTests/Person",
           "/home/daniel/Repositories/ruffusTests/MyTests/Place"]
    files =[]
    for folder in folders:
        for file in os.listdir(folder):
            if file.endswith(".csv"):
                files.append(os.path.join(folder,file))
    return files

starting_files =  getFiles()
#print (starting_files)

# STAGE 1 .csv -> .st1
@transform(input=starting_files,filter=suffix(".csv"),output=".st1")
def summarize_entity_names (input_file, output_file):
    tasks.summarize_entity_names(input_file, output_file)

# STAGE 2 .cst1 -> .st2
@transform(input=summarize_entity_names,filter=suffix(".st1"),output=".st2")
def get_wikipedia_pages (input_file, output_file):
    print("Doing: %s"%(input_file))
    tasks.get_wikipedia_page(input_file, output_file)
    print("Done")

# STAGE 3 .cst2 -> .st3
@transform(input=get_wikipedia_pages,filter=suffix(".st2"),output=".st3")
def get_article_plain_text (input_file, output_file):
    tasks.get_wikipedia_plain_text(input_file, output_file)

# STAGE 3 .cst3 -> .st4
@transform(input=get_article_plain_text,filter=suffix(".st3"),output=".st4")
def split_sentences (input_file, output_file):
    tasks.sentence_splitting(input_file, output_file)

# STAGE 4 .cst4 -> .st5
@transform(input=split_sentences,filter=suffix(".st4"),output=".st5")
def filter_sentences_with_mentions (input_file, output_file):
    tasks.filter_sentences_with_entities(input_file, output_file)

# STAGE 5 .cst5 -> .st6
@transform(input=filter_sentences_with_mentions,filter=suffix(".st5"),output=".st6",extras=[{"splitter":tasks.split_words}])
def split_sentence_and_entitites (input_file, output_file,extras):
    tasks.split_sentences_entities(input_file, output_file,extras["splitter"])
    
# STAGE 6 .cst6 -> .st7
@transform(input=split_sentence_and_entitites, filter=suffix(".st6"),output=".st7")
def annotate_entities (input_file, output_file):
    tasks.annotate_sentences_entities(input_file,output_file)

# STAGE 7 .cst7 -> .conllu
@transform(input=annotate_entities, filter=suffix(".st7"),output=".conllu")
def make_IOB (input_file, output_file):
    tasks.IOB(input_file,output_file)



# STAGE 5 .cst5 -> .st6
# In the first element of the extras recieves a list of dictionarys of postaggers in which the key is the postagger name and the value the actual postagger function
# The postagger function must return a dictionary with two keys: 'tokens' and 'tags'. The first one containing the tokens created by the parser, the second containing tha tags
# The postagger functions are in the file 'postaggers.py'
# @transform(input=filter_sentences_with_mentions,filter=suffix(".st5"),output=".st6",extras=[[{"Polyglot":postaggers.polyglot_postagger}]])
# def annotate_postag (input_file, output_file,extras):
#     postaggers = extras[0]
#     tasks.annotate_sentences_with_postaggers(input_file, output_file, postaggers)

# MERGE between stages  & 2 .st1 ->  chunk.st1
# @split(summarize_entity_names,".st1chunk")
# def merge_csv_files(input_file, output_file,xtra={"split_size":10}):
#     pass

#STAGE 4 chunk[].st2 -> .st3
# @transform(get_wikipedia_pages,
#            suffix(".st2"),
#            ".st3")
# def match_entities (input_file, output_file):
#     tasks.match_entities(input_file, output_file)

# [deprecated] STAGE 2 SUBDIVIDE the files in chunks .st1 -> .cst1
# chunk_extension = ".cst1"
# @subdivide(summarize_entity_names,
#            formatter(),
#            "{path[0]}/{basename[0]}.*"+chunk_extension,
#            "{path[0]}/{basename[0]}")
# def subdivide_csv_files(input_file, output_files, output_file_name_stem):
#     tasks.split_csv_file(input_file,output_file_name_stem,10,chunk_extension)


#pipeline_run(["summarize_entity_names","split_csv_files"],forcedtorun_tasks=["summarize_entity_names","split_csv_files"])
#pipeline_run(["summarize_entity_names","subdivide_csv_files","get_wikipedia_pages"],forcedtorun_tasks=["summarize_entity_names","subdivide_csv_files","get_wikipedia_pages"])

if False:
    pipeline_run(verbose=1,multiprocess=1)
else:
    #pipeline_run()
    pipeline_run(["split_sentence_and_entitites","annotate_entities","make_IOB"],
    forcedtorun_tasks=["split_sentence_and_entitites","annotate_entities","make_IOB"])