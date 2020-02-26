#!/usr/bin/env python
# coding: utf-8

# ## COGS Transform Pack:
# **This basic Jupyter Notebook is intended to aid the COGS transformation process(es) of human readable to machine readable data. This Notebook will be used to house the Python definitions and functions that can be imported into the main transform script. Longer term, if deemed useful these functions could form the foundation of a developed Python library.**
# ### Section: Define and Load into Memory Notebook Python Libraries / Components:

# In[1]:


#conda install -c conda-forge cchardet
#!pip install chardet


# **Note: Future update to include Python Doc Strings etc.**

# In[2]:


# Doc Strings / About etc:


# In[3]:


get_ipython().system('pip install csvwlib')
from csvwlib import CSVWConverter

get_ipython().system('pip install slug')
import slug

get_ipython().system('pip install goodtables')
from goodtables import validate
from goodtables import Inspector
inspector = Inspector()

get_ipython().system('pip install PyGithub')
from github import Github

#!pip install PyDrive

from pprint import pprint

import pandas as pd

import numpy as np

import json

from IPython.display import Markdown, display
from IPython.display import display
from IPython.display import clear_output

from ipywidgets import IntProgress
import time


# In[4]:


# Function for markdown Notebook outputs:
# Paramaters: string (string) - string to display, colour (String) - colour of output:
# Dependencies:
def printmd(string, colour=None):
    colourstr = "<span style='color:{}'>{}</span>".format(colour, string)
    display(Markdown(colourstr))
    return
    


# In[5]:


# Function that determines Python Execution Environment:
# Dependencies: printmd
def boo_pythonNB_environment():
    if get_ipython().__class__.__name__ == 'ZMQInteractiveShell':
        boo_pythonNB_environment = True
    else:
        boo_pythonNB_environment = False

    printmd('***Execution Environment: ' + get_ipython().__class__.__name__ + '; setting boo_pythonNB_environment to: '           + str(boo_pythonNB_environment) + '.***', colour='Grey')
    
    return boo_pythonNB_environment


# In[6]:


# Simple display of DataFrame Collections.
# Paramaters: dataframe_collection, row_display_limit blah blah to update...
# Dependencies: printmd
def display_DF_collection(dataframe_collection, str_title = None, row_display_limit = 10):
    if str_title != None:
        printmd("\n" + "="*115, colour='Grey')
        printmd('**' + str_title + '**' + ' ', colour = 'Magenta')
        printmd("="*115, colour='Grey')       
    for key in dataframe_collection.keys():
        printmd("\n" + "="*115, colour='Grey')
        printmd('**' + key + '**' + ' *: First ' + str(row_display_limit) + ' records displayed of*' + ' ' + str(dataframe_collection[key].shape[0]) + ' records.', colour='Blue')
        printmd("="*115, colour='Grey')
        #print(dataframe_collection[key]) #Print like this for Logs...  
        display(dataframe_collection[key].head(row_display_limit))


# In[7]:


# Simple display of DataFrame Collections.
# Paramaters: dataframe_collection, row_display_limit blah blah to update...
# Dependencies: printmd, slug
def display_DF_collection_csv_report(dataframe_collection):
    for key in dataframe_collection.keys():
        csv_path_file = str(slug.slug(key)) + '.csv'
        dataframe_collection[key].to_csv(csv_path_file, index = None, header=True)
        report = validate(csv_path_file)
        printmd("\n" + "="*115, colour='Grey')
        printmd('***CSV extract for: ' + csv_path_file + ' has been examined. Validation of the file is:***' + ' ' + str(report['valid']) + '.', colour='Purple')
        printmd("="*115, colour='Grey')
        if report['valid'] == False:
            closer_file_inspection = inspector.inspect(csv_path_file)
            pprint(closer_file_inspection)


# In[8]:


# Obtain all data elements (columns) from data outputs (tidy data):
# Paramaters: dataframe_collection blah blah to update...
# Dependencies: printmd
def get_component_elements(dataframe_collection):
    codelist_cols = []
    i = 0
    for key in dataframe_collection.keys():
        codelist_cols.append((list(dataframe_collection[key].columns)))
        printmd('**Extracted columns from dataset ' + key + ":**", colour='Green')
        printmd(codelist_cols[i], colour='Grey')  
        i += 1
    return codelist_cols


# In[9]:


# Generate a collection of DataFrames - default all entries to dimensions intentionally
# - they need to be assigned manually.
# Dependencies:
def get_transform_component_schema(dataframe_collection, codelist_cols):
    dataframe_elements_collection = {}
    i = 0
    for key in dataframe_collection.keys():
        for x in range(len(codelist_cols[i])):
            df_components = pd.DataFrame(codelist_cols[i],columns=[key])
        df_components['Entry Type']='Dimension'
        dataframe_elements_collection[key] = df_components
        i += 1
    return dataframe_elements_collection


# In[10]:


# Assign collection of DataFrame components to their type (Measure or Observation):
# Code is in-efficient due to multiple passes of the collections per call.
# Original code was contained in a single loop but this component has easier usability...
# future code revision could improve performance by str_components and str_type_components being passed as arrays.
# Dependencies:
def assign_transform_component_type(dataframe_elements_collection, str_component, str_type_component):
    switch = 'Proceed'
    if (str_type_component == 'Measure') or (str_type_component == 'M') or (str_type_component == 'm'):
        type_component = 'Measure'
    elif (str_type_component == 'Observation') or (str_type_component == 'O') or (str_type_component == 'o'):
        type_component = 'Observation'
    else:
        switch = 'Error'
    if switch != 'Error':
        for key in dataframe_elements_collection.keys():
            dataframe_elements_collection[key].loc[dataframe_elements_collection[key][key] == str_component, ['Entry Type']] = type_component
    return dataframe_elements_collection
    


# In[11]:


# Generate codelists from a collection of DataFrames - using assigned dimenison element type.
# Dependencies:
def get_codelists_from_dimensions(dataframe_collection, dataframe_elements_collection):
    dataframe_codelists_collection = {}
    for key in dataframe_elements_collection.keys():
        df_result = pd.DataFrame(columns = ['DropMe'])

        df_temp = dataframe_elements_collection[key].loc[dataframe_elements_collection[key]['Entry Type'] == 'Dimension']
        #display(df_temp)
        codelist_cols = []
        for rows in df_temp.itertuples():
            codelist_cols_temp = rows[1]
            codelist_cols.append(codelist_cols_temp)


        for col in dataframe_collection[key]:
            if col in codelist_cols:
                my_codelist_lst = dataframe_collection[key][col].unique()
                df_codelist = pd.DataFrame(my_codelist_lst, columns = [col])
                df_result = pd.concat([df_result, df_codelist], axis = 1, ignore_index=False, sort=False)
        df_result = df_result.drop('DropMe', 1)
        dataframe_codelists_collection[key] = df_result
    return dataframe_codelists_collection
        


# In[12]:


# Join our Slugized transformed data with our REF data - using DataFrames only - no CSV functionality:
# Dependencies: 
def __compute_REFData_with_Transform__(input_df, source_ref_columns_df: pd.DataFrame, source_ref_components_df: pd.DataFrame):
    
    REFdata_intermediate = pd.merge(input_df, source_ref_columns_df, left_on='REFColumnsCSV Link', right_on='title', how='inner')
    REFdata_linked_successful = pd.merge(REFdata_intermediate, source_ref_components_df, left_on='title', right_on='Label', how='left')
    
    REFdata_linked_UNsuccessful = pd.merge(input_df, source_ref_columns_df, left_on='REFColumnsCSV Link', right_on='title', how='outer', indicator=True).query('_merge=="left_only"')    

    return REFdata_linked_successful, REFdata_linked_UNsuccessful


# In[13]:


# Prepares a copy of the data elements to modify. We need to keep the original data elements for later processing.
# Remember: we're passing collections of DataFrames (which hold the data elements).
# Purpose - useability of Notebooks - reduces loops and complexity.
# The REFColumnsCSV_Link was originally in place as I presumed incorrectly we were matching with Slugized Elements.
# Dependencies:
def get_mapped_elements(dataframe_elements_collection):
    dataframe_mapped_elements_collection = {}
    # Prepare mapping in memory component:
    for key in dataframe_elements_collection.keys(): 
        dataframe_mapped_elements_collection[key] = dataframe_elements_collection[key].copy()
        for cols in dataframe_mapped_elements_collection[key][key]:
            idx_temp = dataframe_mapped_elements_collection[key].index[dataframe_mapped_elements_collection[key][key] == cols]
            dataframe_mapped_elements_collection[key].loc[idx_temp, 'REFColumnsCSV Link'] = cols #slug.slug(cols) Not matched on Slugilzed as first believed!

    return dataframe_mapped_elements_collection


# In[14]:


# Automated mapping of transformed components with the master reference data:
# Dependencies: get_mapped_elements, align_REFdata_with_Transform
def map_REFData_with_Transform(dataframe_elements_collection, df_ref_repo_columns: pd.DataFrame, df_ref_repo_components: pd.DataFrame, execute_flag = 'AutoMap'):
    dataframe_mapped_elements_collection = {}
    dataframe_mapped_elements_collection_errors = {}

    if execute_flag == 'AutoMap':
        dataframe_mapped_elements_collection = get_mapped_elements(dataframe_elements_collection)
    
    if execute_flag != 'AutoMap':
        dataframe_mapped_elements_collection = dataframe_elements_collection
    
    for key in dataframe_mapped_elements_collection.keys():
        df_collection_point, df_collection_point_errors = __compute_REFData_with_Transform__(dataframe_mapped_elements_collection[key], df_ref_repo_columns, df_ref_repo_components)
        dataframe_mapped_elements_collection[key] = df_collection_point
        dataframe_mapped_elements_collection_errors[key] = df_collection_point_errors  
    
    return dataframe_mapped_elements_collection, dataframe_mapped_elements_collection_errors
    


# In[15]:


# Manually map the transform component with the user defined reference data element:
# Dependencies: slug
def assign_reference_data_mapping(dataframe_collection, str_component, str_component_new_map, slug_it = False):
    if slug_it == True:
        str_component_new_map = slug.slug(str_component_new_map)
    for key in dataframe_collection.keys():
        dataframe_collection[key][key].loc[dataframe_collection[key][key] == str_component] = str_component_new_map
    return dataframe_collection
   


# In[16]:


# Create the JSON meta file table schema - WARNING! This is a seriously hacked up <deleted swear words!>. It needs refactoring ASAP:
# Dependencies: __get_meta_json_table_schema
def __get_meta_json_table_schema(dataframe_mapped_elements_collection, json_meta_data_collection):

    for key in json_meta_data_collection.keys():
        scary_movie_string = ''
        for idx, val in enumerate(dataframe_mapped_elements_collection[key].itertuples()):
            scary_movie_string = scary_movie_string + ('\n{ \n"title": '                                 + str(dataframe_mapped_elements_collection[key].title[idx]).rstrip()                                 + '",')
            scary_movie_string = scary_movie_string + ('\n"name": '                                 + str(dataframe_mapped_elements_collection[key].name[idx]).rstrip()                                 + '",')
            if pd.notna(dataframe_mapped_elements_collection[key].Codelist[idx]):
                scary_movie_string = scary_movie_string + ('\n"propertyUrl": '                                     + str(dataframe_mapped_elements_collection[key].Codelist[idx]).rstrip()                                     + '",')
            if pd.notna(dataframe_mapped_elements_collection[key].regex[idx]):
                scary_movie_string = scary_movie_string + ('\n"datatype": {"format": "'                                     + str(dataframe_mapped_elements_collection[key].regex[idx]).rstrip()                                     + '"},')
            scary_movie_string = scary_movie_string + ('\n"required": true \n},')

        json_meta_data_collection[key] = json_meta_data_collection[key] + scary_movie_string[:-1]
        json_meta_data_collection[key] = json_meta_data_collection[key] + ('\n]\n}\n}')

    return json_meta_data_collection


# In[17]:


# Create the JSON meta file - WARNING! This is a seriously hacked up <deleted swear words!>. It needs refactoring ASAP:
# Dependencies: __get_meta_json_table_schema
def create_meta_json(dataframe_mapped_elements_collection, dataframe_mapped_elements_collection_names, str_distribution_meta):
    
    json_metadata_collection = {}
    
    json_metadata_string_initial = ('{ \n"@context": "http://www.w3.org/ns/csvw", ') # {"@language": "en"}') # \n],')
    str_distribution_meta = str_distribution_meta.replace("\'", "\"")
    str_distribution_meta_pt2 = str_distribution_meta.split('"')

    str_parsed_meta = []
    for i in range(len(str_distribution_meta_pt2)):
        if i % 2 != 0:
            str_parsed_meta.append(str_distribution_meta_pt2[i])

    str_parsed_meta_links = {}
    for i in range(len(str_parsed_meta)):
        if (str_parsed_meta[i][:7] != 'http://') and (str_parsed_meta[i+1][:7] == 'http://'):
            str_parsed_meta_links[str_parsed_meta[i]] = str_parsed_meta[i+1]

    hacked_string_baby = ''
    for key in str_parsed_meta_links.keys():
        hacked_string_baby = hacked_string_baby + ('\n"' + str(key).rstrip() + '": ' + '"' + str(str_parsed_meta_links[key]).rstrip() + '", ').rstrip()

    for entry in range(len(dataframe_mapped_elements_collection_names)): 
        json_metadata_collection[dataframe_mapped_elements_collection_names[entry]] = json_metadata_string_initial + hacked_string_baby + '\n"url: "' + dataframe_mapped_elements_collection_names[entry] + '",' + '\ntableSchema": { \n"columns": ['
    
    table_schema = __get_meta_json_table_schema(dataframe_mapped_elements_collection, json_metadata_collection)
    
    return json_metadata_collection
   


# In[18]:


# Create the RDF - I'm sure it is possible to use a local file (file://) but couldn't get it working.
# Therefore, since this whole section of the pipeline requires a refactor I'm just using the RDF HTTP request.
# GIT is where the CSV and META will be pushed for the RDF to be created.
# Dependencies: 
def create_rdf(dataframe_collection):

    my_pb_max = len(dataframe_collection)
    my_pb1_calc = 0
    
    for key in dataframe_collection.keys():
        
        str_csv_for_rdf = str(slug.slug(key) + '.csv')
        str_meta_for_rdf = str_csv_for_rdf[:-4] + '-metadata.json'
        str_file_for_rdf = str_csv_for_rdf[:-4] + '.rdf'

        clear_output(wait=True)
        my_pb2_calc = 0
        
        printmd("\n" + "="*115, colour='Grey')
        printmd('RDF Component Processing:', colour='Green')
        img_prgress_bar_1 = IntProgress(min = 0, max = my_pb_max, description = 'Total:') # instantiate the bar
        img_prgress_bar_2 = IntProgress(min = 0, max = 6, description = 'Stage:') # instantiate the bar
        display(img_prgress_bar_1)
        display(img_prgress_bar_2)
        printmd("\n" + "="*115, colour='Grey')

        img_prgress_bar_1.value = my_pb1_calc # signal to increment the progress bar

        
        printmd('Identifying mandatory files: ' + str_csv_for_rdf + '--' + str_meta_for_rdf + '--' + str_file_for_rdf + '...', colour = 'Blue')
        my_pb2_calc += 1
        img_prgress_bar_2.value = my_pb2_calc

        token=("aa16f221b7d3168db8053bd74d4a6fabac79de60") # For one user who has access to one GIT directory.
        g=Github(login_or_token=token)#,base_url=url)
        #display(g)
        g.get_user()
        for repo in g.get_user().get_repos():
            if repo.name == 'Pipeline_Processing': #print('Found Repository: ' + repo.name)
                myRepo = repo
                printmd('GIT Repository Identified: ' + str(myRepo) + '...', colour = 'Red')
                my_pb2_calc += 1
                img_prgress_bar_2.value = my_pb2_calc
                break

        printmd('Preparing files for processing: ' + str_csv_for_rdf + '...', colour = 'Blue')
        file_csv_for_rdf = open(str_csv_for_rdf, "r")

        printmd('Preparing files for processing: ' + str_meta_for_rdf + '...', colour = 'Blue')
        file_meta_for_rdf = open(str_meta_for_rdf, "r")

        printmd('Preparing files for processing: ' + str_file_for_rdf + '...', colour = 'Blue')    
        file_rdf_package = open(str_file_for_rdf, "w")

        my_pb2_calc += 1
        img_prgress_bar_2.value = my_pb2_calc
        
        myRepo.create_file(str_csv_for_rdf, ("content = Upload RDF-Processing for: " + str_csv_for_rdf), file_csv_for_rdf.read()) #, branch="master")
        myRepo.create_file(str_meta_for_rdf, ("content = Upload RDF-Processing for: " + str_meta_for_rdf), file_meta_for_rdf.read()) #, branch="master")

        my_pb2_calc += 1
        img_prgress_bar_2.value = my_pb2_calc        
        
        rdf_package = CSVWConverter.to_rdf(str('https://raw.githubusercontent.com/GSS-Cogs/Pipeline_Processing/master/' + str_csv_for_rdf), format='ttl')
        file_rdf_package.write(rdf_package)
        file_rdf_package.close()
        printmd('RDF Generation Successful for: ' + str_file_for_rdf + '.', colour = 'Green')

        my_pb2_calc += 1
        img_prgress_bar_2.value = my_pb2_calc 
        
        printmd('Repository Clean-up started: ' + str(myRepo) + '...', colour = 'Blue')
        contents = repo.get_contents(str_csv_for_rdf)#, ref="RDF")
        myRepo.delete_file(contents.path, "Clean Down", contents.sha, branch="master")
        contents = repo.get_contents(str_meta_for_rdf)#, ref="RDF")
        myRepo.delete_file(contents.path, "Clean Down", contents.sha, branch="master")

        my_pb2_calc += 1
        img_prgress_bar_2.value = my_pb2_calc 
        
        my_pb1_calc += 1 # Overall Progress.
        time.sleep(2) # 2 secs - Really?

    clear_output(wait=True)
    printmd("\n" + "="*115, colour='Grey')
    printmd('RDF Component Processing:', colour='Green')
    img_prgress_bar_1 = IntProgress(min = 0, max = 1, description = 'Total:') # instantiate the bar
    img_prgress_bar_2 = IntProgress(min = 0, max = 1, description = 'Stage:') # instantiate the bar
    display(img_prgress_bar_1)
    display(img_prgress_bar_2)
    img_prgress_bar_1.value = 1
    img_prgress_bar_2.value = 1
    printmd('***Process COMPLETE!***', colour = 'Green')
    printmd("\n" + "="*115, colour='Grey')


    return None # Refactor to report status?


# In[ ]:




