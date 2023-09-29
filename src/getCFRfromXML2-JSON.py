import os
import logging
import json
from datetime import datetime
import pandas as pd
import xml.etree.ElementTree as eTree

def initializeDataFrame():
    # Define the column names
    column_names = ["section_no", "subject", "text", "modal_verb", "deontic_operator", "antecedent", "consequent"]
    # Load the list of lines into a DataFrame
    iDF = pd.DataFrame(columns=column_names)
    # Change the display options to show more text
    pd.set_option('max_colwidth', 80)
    return iDF


def print_list_of_provisions(cfr_list):
    df = initializeDataFrame()
    my_provision_list = []
    for subpart in cfr_list:
        row_data = {}
        for key, value in subpart.items():
            row_data[key] = value
        my_provision_list.append(row_data)
        df = pd.DataFrame(my_provision_list)
    return df


def getCFRData(reg_xml_file_location):

    try:
        # use XSLT to get only two important elements
        # store the results and return
        tree = eTree.parse(reg_xml_file_location)
        root = tree.getroot()
        cfrXmlElements = []

        # loops through the CFR.xml file to get the 
        # section number, subject, and text (key/value) pairs- dictionary
        # stores them in a list.  See the following example
        # 'SECTNO': '1.000', 'SUBJECT': 'Scope of part.', 'TEXT': 'This part sets forth basic policies...
        # this method returns a list of dictionaries

        for subpart_element in root.findall('.//SECTION'):
            subpart_element_data = {
                'SECTNO': '',
                'SUBJECT': '',
                'TEXT': ''
            }

            sectno_elements = subpart_element.findall('.//SECTNO')
            if len(sectno_elements) > 0:
                subpart_element_data['SECTNO'] = ", ".join(
                    [element.text for element in sectno_elements if element.text is not None])

            subject_elements = subpart_element.findall('.//SUBJECT')
            if len(subject_elements) > 0:
                subpart_element_data['SUBJECT'] = ", ".join(
                    [element.text for element in subject_elements if element.text is not None])

            text_elements = subpart_element.findall('.//P')
            if len(text_elements) > 0:
                text = ""
                for element in text_elements:
                    text_fragments = [text_fragment.strip() for text_fragment in element.itertext()]
                    text += " ".join(text_fragments) + " "
                text = " ".join(text.split())  # remove extra whitespace
                subpart_element_data['TEXT'] = text

            cfrXmlElements.append(subpart_element_data)
        return cfrXmlElements
    except() as e:
        logging.error("Error occurred while parsing XML: %s", e)
        return []


def save_dataframe_as_excel(dff, dataSetName, cfrHomeBase):
    
    now = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    fileExt = ".xlsx"
    output = "output/"

    # Define the file name and path
    file_name = cfrHomeBase + output + dataSetName + now + fileExt

    # Create the output directory if it doesn't exist
    output_dir = os.path.dirname(file_name)
    os.makedirs(output_dir, exist_ok=True)
    

    try:
        # Save using XlsxWriter
        writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
        dff.to_excel(writer, index=False, sheet_name='SHAMROQ')
        workbook  = writer.book
        worksheet = writer.sheets['SHAMROQ']
    
        # Specify the format for text
        text_format = workbook.add_format({'num_format': '@'})

        # Apply the format
        worksheet.set_column('A:A', None, text_format)
        writer.save()

    except Exception  as e:
        logging.error("Cannot save file: %s", e)
        file_name = None

    return file_name

def save_dataframe_as_csv(dff, dataSetName, cfrHomeBase):
    now = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    fileExt = ".csv"
    output = "output/"

    # Define the file name and path
    file_name = cfrHomeBase + output + dataSetName + now + fileExt

    # Create the output directory if it doesn't exist
    output_dir = os.path.dirname(file_name)
    os.makedirs(output_dir, exist_ok=True)

    try:
        # Write the DataFrame to a CSV file
        dff.to_csv(file_name, index=False, encoding='utf-8-sig')
    except OSError as e:
        logging.error("Cannot save file: %s", e)
        file_name = None

    return file_name


def extract_cfr_data(regList):
    # Initialize an empty DataFrame
    df_all_regulations = pd.DataFrame()

    # Loop through the list of CFR.xml files
    for regXmlInstance in regList:
        regSectionElements = getCFRData(regXmlInstance)
        df_regulation = pd.DataFrame(regSectionElements)
        df_all_regulations = pd.concat([df_all_regulations, df_regulation], ignore_index=True)

    # Force columns to desired data types - strings - to prevent excel warnings
    for col in df_all_regulations.columns:
        df_all_regulations["SECTNO"] = df_all_regulations["SECTNO"].apply(lambda x: f"{x}\u00A0")

    return df_all_regulations


def init(cfr_with_year):
    # Configure logging
    logging.basicConfig(filename='./logs/app.shamroq.log', level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    # Set config file location
    configFile = "./config/config.json"
    logging.info("Location of Config File: %s", configFile)

    # Load configuration from JSON file
    config = None;
    try:
        # Load configuration from JSON file
        with open(configFile, 'r', encoding='utf-8') as config_file:
            config = json.load(config_file)
    except FileNotFoundError as e:
        logging.error("Config file not found: %s", e)
    except json.JSONDecodeError as e:
        logging.error("Error parsing JSON file: %s", e)
    except Exception as e:
        logging.error("An error occurred while loading configuration: %s", e)

    # extract REG_NAME, BASE URL, & CFR volumes from config file
    cfr_home_base = config[cfr_with_year]['HOME_BASE']
    cfr_volumes = config[cfr_with_year]['VOLUMES']
    cfr_reg_name = config[cfr_with_year]['REG_NAME']

    cfr_xml_data = [cfr_home_base + volume for volume in cfr_volumes]
    logging.info("Final dataset: %s", cfr_xml_data)
    return cfr_xml_data, cfr_reg_name, cfr_home_base


def main():
    # CFR_WITH_YEAR = "CFR_16_2021"
    # CFR_WITH_YEAR = "CFR_45_2021"
    CFR_WITH_YEAR = "CFR_48_2021"
    dataSet, regName, homeBase = init(CFR_WITH_YEAR)
    df_regs = extract_cfr_data(dataSet)
    logging.info("List of regulations: %s", df_regs)
    cfr_extracted_file = save_dataframe_as_excel(df_regs, regName, homeBase)
    logging.info("Saved File Name: %s", cfr_extracted_file)



# -------------------------------------
# @Author Patrick Cook
# @Date: circa 2023 ATLAS release
# PREPROCESSING:  getCFRfromXML2-JSON.py
# Uses the getCFRData function to extract the SECTNO, SUBJECT,
# and concatenated TEXT elements within each SECTION element from all seven
# volumes of Title 48 contained in XML files.
# -------------------------------------

if __name__ == '__main__':
    start_time = datetime.now()
    main()
    end_time = datetime.now()
    duration = end_time - start_time
    logging.info("Execution time: %s", duration)
    print(f"Execution time: {duration}")
    




