from ruffus import *
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

# STAGE 2 SUBDIVIDE the files in chunks .st1 -> .cst1
chunk_extension = ".cst1"
@subdivide(summarize_entity_names,
           formatter(),
           "{path[0]}/{basename[0]}.*"+chunk_extension,
           "{path[0]}/{basename[0]}")

def subdivide_csv_files(input_file, output_files, output_file_name_stem):
    tasks.split_csv_file(input_file,output_file_name_stem,10,chunk_extension)
    
#STAGE 3 .cst1 -> .st2
@transform(subdivide_csv_files,suffix(chunk_extension),".st2")
def get_wikipedia_pages (input_file, output_file):
    print(input_file)
    tasks.get_wikipedia_page(input_file, output_file)

# MERGE between stages  & 2 .st1 ->  chunk.st1
# @split(summarize_entity_names,".st1chunk")
# def merge_csv_files(input_file, output_file,extra={"split_size":10}):
#     pass


#STAGE 4 chunk[].st2 -> .st3
# @transform(get_wikipedia_pages,
#            suffix(".st2"),
#            ".st3")
# def match_entities (input_file, output_file):
#     tasks.match_entities(input_file, output_file)

#pipeline_run(["summarize_entity_names","split_csv_files"],forcedtorun_tasks=["summarize_entity_names","split_csv_files"])


#pipeline_run(["summarize_entity_names","subdivide_csv_files","get_wikipedia_pages"],forcedtorun_tasks=["summarize_entity_names","subdivide_csv_files","get_wikipedia_pages"])

pipeline_run(["subdivide_csv_files","get_wikipedia_pages"],
forcedtorun_tasks=["subdivide_csv_files","get_wikipedia_pages"],
multithread=50,
multiprocess=4)

#if __name__ == "main":
#    pipeline_run(forcedtorun_tasks=["match_entities"],target_tasks=["match_entities"])
