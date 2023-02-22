from typing import List
import gspread
import pandas as pd


def format_sentences(sentences: List, prefix: str):
    """
    :param sentences: item or product names
    :param prefix: instruction prefix ex: 'Represent the sentence for retrieving supporting documents'
    :return:List[List] ex: [Represent the sentence for retrieving supporting documents;, cost of amazon prime]
    """
    formatted_sentences = []
    for sentence in sentences:
        formatted_sentences.append([f"{prefix}", sentence])
    return formatted_sentences


def get_spreadsheet_data(spreadsheet_id: str) -> pd.DataFrame:
    """
    Create an authorized client, Open the spreadsheet by ID and Get spreadsheet's worksheets
    Create a list to hold the values and  create df from values
    """
    client = gspread.oauth()
    spreadsheet = client.open_by_key(spreadsheet_id)
    worksheets = spreadsheet.worksheets()
    values = []
    for ws in worksheets:
        values.extend(ws.get_all_records())
    df = pd.DataFrame(values)
    return df


def concat_columns(df: pd.DataFrame, delimiter: str = ', ') -> list[str]:
    """
    :param df: pass the training dataframe
    :param delimiter: choose the delimiter according it the task ex: ", "
    :return: list of concatenated strings
    """
    # remove nan
    df = df.fillna('')
    # Create an empty list to store the concatenated values
    concatenated = []
    # Iterate over the rows of the dataframe
    for index, row in df.iterrows():
        # Concatenate the values of the name, group_name_1, and group_name_2 columns
        concat_string = f"{row['name']}{delimiter}{row['group_name']}{delimiter}{row['group_name_2']}"
        # Remove any extra whitespace
        cleaned_string = ' '.join(concat_string.split())
        # Add the cleaned string to the list of concatenated values
        concatenated.append(cleaned_string)
    # Return the list of concatenated strings
    return concatenated


def concat_columns_combinations(df: pd.DataFrame, delimiter: str = '$') -> list[tuple]:
    # Create an empty list to store the concatenated values
    concatenated = []
    df = df.fillna('')
    # Iterate over the rows of the dataframe
    for index, row in df.iterrows():
        # Concatenate the values of the name, group_name_1, and group_name_2 columns
        concat_string_1 = f"{row['name']}{delimiter}{row['group_name']}{delimiter}{row['group_name_2']}"
        concat_string_2 = f"{row['name']}{delimiter}{row['group_name_2']}{delimiter}{row['group_name']}"
        concat_string_3 = f"{row['group_name']}{delimiter}{row['name']}{delimiter}{row['group_name_2']}"
        concat_string_4 = f"{row['group_name']}{delimiter}{row['group_name_2']}{delimiter}{row['name']}"
        concat_string_5 = f"{row['group_name_2']}{delimiter}{row['name']}{delimiter}{row['group_name']}"
        concat_string_6 = f"{row['group_name_2']}{delimiter}{row['group_name']}{delimiter}{row['name']}"

        # Remove any extra whitespace
        cleaned_string_1 = ' '.join(concat_string_1.split())
        cleaned_string_2 = ' '.join(concat_string_2.split())
        cleaned_string_3 = ' '.join(concat_string_3.split())
        cleaned_string_4 = ' '.join(concat_string_4.split())
        cleaned_string_5 = ' '.join(concat_string_5.split())
        cleaned_string_6 = ' '.join(concat_string_6.split())

        # Add the cleaned strings to the list of concatenated values as tuples with index
        concatenated.extend(
            [(index + 1, cleaned_string_1), (index + 1, cleaned_string_2), (index + 1, cleaned_string_3),
             (index + 1, cleaned_string_4), (index + 1, cleaned_string_5), (index + 1, cleaned_string_6)])

    # Return the list of concatenated strings as tuples
    return concatenated
