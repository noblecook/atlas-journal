import os
import logging
import json
import time
import spacy
import pandas as pd
import srsly
from datetime import datetime
nlp = spacy.load("en_core_web_lg")


linguisticFeatures = ['TEXT', 'PATTERN', 'SPAN', 'SUBJ', 'VERB', 'OBJECT']
df = pd.DataFrame(columns=linguisticFeatures)
shamroqCfg = "C:/Users/patri/PycharmProjects/research/PyDevShamroq/config/shamroq-patterns-rules.jsonl"


'''
Adding items to the nlp pipeline
https://spacy.io/usage/processing-pipelines
'''
patterns = srsly.read_jsonl(shamroqCfg)
ruler = nlp.add_pipe("span_ruler")
ruler.add_patterns(patterns)


def getTimeNow():
    t = time.localtime()
    current_time = time.strftime("%c", t)
    print("Current Time =", current_time)
    return t


# https://demos.explosion.ai/matcher
def classifySpan(text):
    try:
        doc = nlp(text)
        predication = None
        for span in doc.spans["ruler"]:
            predication = span.label_, span.text
        return predication
    except Exception as e:
        # Handle the exception here (e.g., log the error)
        logging.error("Error in classifySpan: %s", str(e))
        raise


def process_regulations2(df_of_regulations):
    result_df = pd.DataFrame(
        columns=["SECTNO", "CFRSubject", "Original_sentence", "Matched_Label", "Matched_Text"])

    for index, row in df_of_regulations.iterrows():
        section_no = row["SECTNO"]
        cfr_subj = row["SUBJECT"]
        for col_name in df_of_regulations.columns:
            if col_name == "TEXT":
                paragraph = row[col_name]
                if isinstance(paragraph, str):
                    doc = nlp(paragraph)
                    try:
                        result_df = process_sentences2(doc, section_no, cfr_subj, result_df)
                    except Exception as e:
                        # Handle the exception here (e.g., log the error)
                        logging.error("Error processing sentences: %s", str(e))
                else:
                    pass
            else:
                pass
    return result_df


def process_sentences2(doc, section_no, cfr_subj, result_df):
    for sent in doc.sents:
        try:
            components = classifySpan(sent.text)
            if components:
                new_row = {
                    "SECTNO": section_no,
                    "CFRSubject": cfr_subj,
                    "Original_sentence": sent,
                    "Matched_Label": components[0],
                    "Matched_Text": components[1]
                }
                new_row_df = pd.DataFrame([new_row], columns=result_df.columns)
                result_df = pd.concat([result_df, new_row_df], ignore_index=True)

        except ValueError as e:
            # Handle the ValueError
            logging.error("ValueError occurred: %s", str(e))

        except TypeError as e:
            # Handle the TypeError
            logging.error("TypeError occurred: %s", str(e))

        except AttributeError as e:
            # Handle the AttributeError
            logging.error("AttributeError occurred: %s", str(e))

        except Exception as e:
            # Handle any other unexpected exceptions
            logging.error("Unexpected Exception occurred: %s", str(e))

    return result_df


def generate_csv_file(cfr_home_base, name, result_df):
    try:
        now = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        fileExt = ".csv"
        results = "results/"
        CLASSIFIED_PREFIX = "CLASSIFIED_"
        eCFR_48 = "_eCFR_48_ALL_"

        # Define the file name and path
        result_csv_file = cfr_home_base + results + CLASSIFIED_PREFIX + name + now + fileExt

        # Create the output directory if it doesn't exist
        output_dir = os.path.dirname(result_csv_file)
        os.makedirs(output_dir, exist_ok=True)

        result_df.to_csv(result_csv_file, index=False)
        return result_csv_file
    except Exception as e:
        logging.error("An error occurred during CSV file generation: %s", str(e))
        raise


def read_csv_file(csv_file):
    try:
        absolute_path = os.path.abspath(csv_file)
        # Extract filename and size
        filename = os.path.basename(absolute_path)
        file_size = os.path.getsize(absolute_path)

        # Log filename and size
        logging.info("Reading CSV file: %s (Size: %d bytes)", filename, file_size)
        df_of_regulations = pd.read_csv(csv_file)
        return df_of_regulations
    except FileNotFoundError as e:
        logging.error("CSV file not found: %s", str(e))
        raise
    except pd.errors.ParserError as e:
        logging.error("Error parsing CSV file: %s", str(e))
        raise


def read_excel_file(xlsx_file):
    try:
        absolute_path = os.path.abspath(xlsx_file)
        # Extract filename and size
        filename = os.path.basename(absolute_path)
        file_size = os.path.getsize(absolute_path)

        # Log filename and size
        logging.info("Reading Excel file: %s (Size: %d bytes)", filename, file_size)
        df_of_regulations = pd.read_excel(xlsx_file)
        return df_of_regulations
    except FileNotFoundError as e:
        logging.error("CSV file not found: %s", str(e))
        raise
    except pd.errors.ParserError as e:
        logging.error("Error parsing CSV file: %s", str(e))
        raise

def init(cfr_with_year):
    # Configure logging
    logging.basicConfig(filename='./logs/app.shamroq.log', level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    # Set config file location
    configFile = "./config/analyze.config.json"
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

    cfr_xml_data = cfr_home_base + cfr_volumes[0]
    logging.info("Final dataset: %s", cfr_xml_data)
    return cfr_xml_data, cfr_reg_name, cfr_home_base

def process_regulations(df_of_regulations):
    result_data = []  # List to hold data for new DataFrame

    for index, row in df_of_regulations.iterrows():
        section_no = row["SECTNO"]
        cfr_subj = row["SUBJECT"]
        
        paragraph = row["TEXT"]
        if isinstance(paragraph, str):
            doc = nlp(paragraph)
            try:
                result_data.extend(process_sentences(doc, section_no, cfr_subj))
            except Exception as e:
                # Handle the exception here (e.g., log the error)
                logging.error("Error processing sentences: %s", str(e))

    # Convert the result_data list to a DataFrame
    result_df = pd.DataFrame(result_data, columns=["SECTNO", "CFRSubject", "Original_sentence", "Matched_Label", "Matched_Text"])
    return result_df

def process_sentences(doc, section_no, cfr_subj):
    data = []  # List to hold data for each sentence

    for sent in doc.sents:
        try:
            components = classifySpan(sent.text)
            if components:
                new_row = {
                    "SECTNO": section_no,
                    "CFRSubject": cfr_subj,
                    "Original_sentence": sent.text,  # Ensure .text is used here
                    "Matched_Label": components[0],
                    "Matched_Text": components[1]
                }
                data.append(new_row)

        except ValueError as e:
            logging.error("ValueError occurred: %s", str(e))
        except TypeError as e:
            logging.error("TypeError occurred: %s", str(e))
        except AttributeError as e:
            logging.error("AttributeError occurred: %s", str(e))
        except Exception as e:
            logging.error("Unexpected Exception occurred: %s", str(e))

    return data


def save_dataframe_as_excel(dff, dataSetName, cfrHomeBase):
    
    now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    fileExt = ".xlsx"
    results = "results/"
    prefix = "_dtg-"

    # Define the file name and path
    file_name = cfrHomeBase + results + dataSetName + prefix +  now + fileExt

    # Create the output directory if it doesn't exist
    output_dir = os.path.dirname(file_name)
    os.makedirs(output_dir, exist_ok=True)
    

    try:
        # Save using XlsxWriter
        writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
        dff.to_excel(writer, index=False, sheet_name='SHAMROQ_RESULTS')
        workbook  = writer.book
        worksheet = writer.sheets['SHAMROQ_RESULTS']
    
        # Specify the format for text
        text_format = workbook.add_format({'num_format': '@'})

        # Apply the format
        worksheet.set_column('A:A', None, text_format)
        writer.save()

    except Exception  as e:
        logging.error("Cannot save file: %s", e)
        file_name = None

    return file_name


def main():
    CFR_WITH_YEAR = "CFR_16_2021"
    # CFR_WITH_YEAR = "CFR_45_2021"
    # CFR_WITH_YEAR = "CFR_48_2021"
    dataSet, regName, homeBase = init(CFR_WITH_YEAR)
    df_of_regulations = read_excel_file(dataSet)
   
    result_df = process_regulations(df_of_regulations)
    result_csv_file = save_dataframe_as_excel(result_df, regName, homeBase)
    # Log filename and size
    logging.info("resulting file: %s", result_csv_file)

    print(result_csv_file)



# -------------------------------------
# @Author Patrick Cook
# @Date: circa 2023 initial release
# ANALYZE:  analyze_CFR_from_CSV-JSON.py
# The module reads the Excel file (i.e., the output from the getCFRfromXML2-JSON module)
# into a data frame.  The main function iterates through each row and
# processes the "TEXT" column by invoking the “classifySpan” function. The output
# is a .csv file that contains each statement and associated deontic expression
# -------------------------------------
if __name__ == '__main__':
    start_time = datetime.now()
    main()
    end_time = datetime.now()
    duration = end_time - start_time
    logging.info("Execution time: %s", duration)
    print(f"Execution time: {duration}")
