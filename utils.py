from typing import Union, List, Tuple, Hashable
import gspread
import pandas as pd
import numpy as np
import uuid


# from oauth2client.service_account import ServiceAccountCredentials


def add_uuids(df, column_name = "id"):
    # Generate a UUID for each row in the DataFrame
    uuids = [uuid.uuid4() for _ in range(len(df))]
    # Add the UUIDs as a new column to the DataFrame
    df[column_name] = uuids
    return df


def format_sentences(sentences: List, prefix: str):
    """
    type sentences: object
    param sentences: item or product names
    param prefix: instruction prefix ex: 'Represent the sentence for retrieving supporting documents'
    return:List[List] ex: [Represent the sentence for retrieving supporting documents;, cost of amazon prime]
    """
    formatted_sentences = []
    for sentence in sentences:
        formatted_sentences.append([f"{prefix}", sentence])
    return formatted_sentences


def get_spreadsheet_data(sheet_identifier: str, use_open: bool = True) -> pd.DataFrame:
    """
    Create an authorized client, Open the spreadsheet by ID and Get spreadsheet's worksheets
    Create a list to hold the values and  create df from values
    param sheet_identifier:
    param use_open:
    return: dataframe
    """
    client = gspread.oauth()
    try:
        if use_open:
            spreadsheet = client.open_by_key(sheet_identifier)
        else:
            spreadsheet = client.open(sheet_identifier)
    except gspread.exceptions.SpreadsheetNotFound:
        raise ValueError(f"Spreadsheet '{sheet_identifier}' not found.")

    worksheets = spreadsheet.worksheets()
    values = []
    for ws in worksheets:
        values.extend(ws.get_all_records())
    df = pd.DataFrame(values)
    return df


def create_sheet(sheet_name: str) -> object:
    gc = gspread.oauth()
    return gc.create(sheet_name)


def read_write_append_gs(sheet_identifier: str, mode: str, data: Union[List[List[str]], pd.DataFrame],
                         worksheet_name: str = None, use_open: bool = True) -> pd.DataFrame:
    """
    Perform read, write, or append operations on a Google Sheets document and return the data as a Pandas DataFrame.

    Parameters:
    sheet_identifier (str): The name or ID of the Google Sheets document.
    mode (str): The mode of operation to perform, which can be 'read', 'write', or 'append'.
    data (list of lists or Pandas DataFrame): The data to write or append to the document. Should be a list of lists, where each inner list represents a row of data.
    credentials_file_path (str): The path to the Google Sheets credentials file.
    worksheet_name (str): The name of the worksheet to use. Optional; if not provided, the first worksheet in the document will be used.
    use_open (bool): Whether to use the `open` method (True) or `open_by_key` method (False) to open the document. Default is True.

    Returns:
    Pandas DataFrame: The data in the Google Sheets document, returned as a Pandas DataFrame.
    """

    # Set up the Google Sheets API credentials
    # scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    # credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_file_path, scope)
    # gc = gspread.authorize(credentials)
    gc = gspread.oauth()
    # Open the Google Sheets document
    try:
        if use_open:
            sheet = gc.open(sheet_identifier)
        else:
            sheet = gc.open_by_key(sheet_identifier)
    except gspread.exceptions.SpreadsheetNotFound:
        raise ValueError(f"Spreadsheet '{sheet_identifier}' not found.")

    if worksheet_name:
        # Open the specified worksheet by name
        worksheet = sheet.worksheet(worksheet_name)
    else:
        # Use the first worksheet in the document if no name is specified
        worksheet = sheet.sheet1

    # Perform the specified mode of operation
    if mode == 'read':
        # Read the data from the document and return as a Pandas DataFrame
        data = worksheet.get_all_values()
        df = pd.DataFrame(data[1:], columns=data[0])
        return df

    elif mode == 'write':
        # Write the data to the document and return as a Pandas DataFrame
        if isinstance(data, pd.DataFrame):
            worksheet.clear()
            data = data.astype(str)
            worksheet.update([data.columns.values.tolist()] + data.values.tolist())
            return data
        elif isinstance(data, list):
            worksheet.clear()
            worksheet.update(data)
            return pd.DataFrame(data[1:], columns=data[0])
        else:
            raise ValueError("Invalid data type for 'write' mode. Data must be a Pandas DataFrame or list of lists.")

    elif mode == 'append':
        # Append the data to the document and return the updated data as a Pandas DataFrame
        if isinstance(data, pd.DataFrame):
            worksheet.append_rows(data.values.tolist())
            updated_data = worksheet.get_all_values()
            df = pd.DataFrame(updated_data[1:])
            # , columns=updated_data[0]
            return df
        elif isinstance(data, list):
            worksheet.append_rows(data)
            updated_data = worksheet.get_all_values()
            df = pd.DataFrame(updated_data[1:])
            # , columns=updated_data[0]
            return df
    # Raise an error if an invalid mode is specified
    else:
        raise ValueError("Invalid mode specified. Mode must be 'read', 'write', or 'append'.")


def concat_columns(df: pd.DataFrame, delimiter: str = ', ') -> List[str]:
    """
    param df: pass the training dataframe
    param delimiter: choose the delimiter according it the task ex: ", "
    return: list of concatenated strings
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


def concat_columns_combinations(df_name: pd.DataFrame, delimiter: str = ', ') -> pd.DataFrame:
    """
    Concatenate the values of the name, group_name_1, and group_name_2 columns in all possible combinations
    param df_name: dataframe name
    param delimiter: delimiter to concat the column values
    return: unique combinations for each
    """
    # Create an empty list to store the concatenated values
    concatenated = []
    seen = set()
    result = []
    df_name = df_name.fillna('')
    # Iterate over the rows of the dataframe
    for _, row in df_name.iterrows():
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

        # uuid for the row
        row_uuid = f"{row['id']}"

        # Add the cleaned strings to the list of concatenated values as tuples with uuid as index
        concatenated.extend(
            [(row_uuid, cleaned_string_1), (row_uuid, cleaned_string_2), (row_uuid, cleaned_string_3),
             (row_uuid, cleaned_string_4), (row_uuid, cleaned_string_5), (row_uuid, cleaned_string_6)])
    # remove the duplicates in the combinations
    for tup in concatenated:
        if tup[1] not in seen:
            result.append(tup)
            seen.add(tup[1])
    # Create a new DataFrame from the list of tuples
    df_new = pd.DataFrame(result, columns=['id', 'name_new'])
    df_new["id"] = df_new["id"].astype(str)
    df_name["id"] = df_name["id"].astype(str)
    # Merge the new DataFrame with the original DataFrame based on the id column
    # Add the columns "manual_match", "category_raw_name", and "source"
    merged_df = df_new.merge(df_name[["id", "manual_match", "category_raw_name", "source"]], on='id')
    # Return the merged DataFrame
    return merged_df


def vlookup(df1: pd.DataFrame, col1: str, df2: pd.DataFrame, col2: str, output_col: str) -> pd.DataFrame:
    """
    Performs a vlookup-like operation on two dataframes and returns a new dataframe with the specified columns.

    Args:
        df1: The first dataframe.
        col1: The name of the column in df1 to merge on.
        df2: The second dataframe.
        col2: The name of the column in df2 to merge on.
        output_col: The name of the column from df2 to include in the output.

    Returns:
        A new dataframe with the specified columns.
    """
    # Perform a merge operation on the two dataframes
    merged_df_new = pd.merge(df1, df2[[col2, output_col]], left_on=col1, right_on=col2, how='left')

    # Drop the duplicate column and rename the output column
    merged_df_new = merged_df_new.drop(col2, axis=1).rename(columns={output_col: f'{output_col}'})

    return merged_df_new


# Function to generate negative samples
def generate_negatives(df: pd.DataFrame) -> pd.DataFrame:
    negative_1 = []
    negative_2 = []
    negative_3 = []
    negative_4 = []
    negative_5 = []
    negative_6 = []
    for i in range(len(df)):
        # Get the positive and its class
        # positive = df.iloc[i]['product_raw_name']
        pos_class = df.iloc[i]['category_raw_name']
        # Filter out rows with the same class as positive
        neg_df = df[df['category_raw_name'] != pos_class]
        # print(neg_df)
        # Get a random negative from the filtered dataframe
        negative_1_text = np.random.choice(neg_df['product_raw_name'])
        negative_2_text = np.random.choice(neg_df['product_raw_name'])
        negative_3_text = np.random.choice(neg_df['product_raw_name'])
        negative_4_text = np.random.choice(neg_df['product_raw_name'])
        negative_5_text = np.random.choice(neg_df['product_raw_name'])
        negative_6_text = np.random.choice(neg_df['product_raw_name'])

        # Append the negative to the negatives list
        negative_1.append(negative_1_text)
        negative_2.append(negative_2_text)
        negative_3.append(negative_3_text)
        negative_4.append(negative_4_text)
        negative_5.append(negative_5_text)
        negative_6.append(negative_6_text)

    # Add the negatives column to the original dataframe
    df['negative_1'] = negative_1
    df['negative_2'] = negative_2
    df['negative_3'] = negative_3
    df['negative_4'] = negative_4
    df['negative_5'] = negative_5
    df['negative_6'] = negative_6

    return df
