from simple_salesforce import Salesforce
import pandas as pd
import yaml

class SalesforceDataFetcher:
    def __init__(self, config_path='config.yml', domain='test'):
        """
        Initializes the SalesforceDataFetcher class with Salesforce credentials from the YAML config file.

        Parameters:
        - config_path: str - Path to the YAML config file containing Salesforce credentials (default: 'config.yml').
        - domain: str - Salesforce domain (default: 'test').
        """
        self.config_path = config_path
        self.domain = domain
        self.sf = None
        self.table_name = None
        self.file_name = None
        self.fields = []

    def load_credentials(self):
        """
        Loads Salesforce credentials from the specified YAML config file.

        Returns:
        - None
        """
        try:
            with open(self.config_path, 'r') as config_file:
                config = yaml.safe_load(config_file)
                self.uname = config['salesforce']['uname']
                self.pwd = config['salesforce']['pwd']
                self.sftoken = config['salesforce']['sftoken']
        except FileNotFoundError as e:
            return f"Error: Config file not found: {e}"
        except yaml.YAMLError as e:
            return f"Error: YAML parsing error: {e}"
        except KeyError as e:
            return f"Error: Missing required key in config file: {e}"
        except Exception as e:
            return f"Error loading credentials: {e}"

    def connect_to_salesforce(self):
        """
        Establishes a connection to Salesforce using the loaded credentials.

        Returns:
        - bool: True if connection is successful, otherwise False.
        """
        try:
            self.sf = Salesforce(username=self.uname, password=self.pwd, security_token=self.sftoken, domain=self.domain)
            return True
        except Exception as e:
            return f"Error connecting to Salesforce please check if you have valid credentials: {e}"

    def fetch_data(self):
        """
        Fetches data from Salesforce for the specified table and fields.

        Returns:
        - pd.DataFrame: DataFrame containing the query results or None if an error occurs.
        """
        if not self.table_name:
            return "Error: Table name not provided. Please set the table name before fetching data."

        fields = self.fields

        # If no fields are provided, fetch all fields from the Salesforce object
        if not fields:
            try:
                object_description = self.sf.__getattr__(self.table_name).describe()
                fields = [field['name'] for field in object_description['fields']]
            except Exception as e:
                return f"Error retrieving fields for {self.table_name}: {e}"

        # Construct SOQL query
        fields_str = ', '.join(fields)
        query = f"SELECT {fields_str} FROM {self.table_name}"

        try:
            # Execute SOQL query
            response = self.sf.query(query)
            df = pd.DataFrame(response['records']).drop(labels='attributes', axis=1, errors='ignore')
            return df
        except Exception as e:
            return f"Error executing query: {e}"

    def save_to_csv(self, df):
        """
        Saves the fetched data to a CSV file.

        Parameters:
        - df: pd.DataFrame - The DataFrame containing the Salesforce data.

        Returns:
        - str: Success or error message.
        """
        if not self.file_name:
            self.file_name = f"{self.table_name}.csv"

        try:
            df.to_csv(self.file_name, index=False)
            return f"Data saved to {self.file_name}"
        except Exception as e:
            return f"Error saving data to file: {e}"

    def fetch_and_save_data(self, table_name=None, fields=None, file_name=None):
        """
        Fetches data from Salesforce and saves it to a CSV file.

        Parameters:
        - table_name: str - The name of the Salesforce table (object) to query. Must be provided.
        - fields: list - List of fields to include in the query.
        - file_name: str - Name of the CSV file to save the results. If not provided, defaults to table_name.csv.

        Returns:
        - str: Success or error message.
        """
        if table_name:
            self.table_name = table_name
        else:
            return "Error: Please provide a table name for fetching data first."

        if fields is not None:
            self.fields = fields

        if file_name is not None:
            self.file_name = file_name

        error = self.load_credentials()
        if error:
            return error

        if not self.connect_to_salesforce():
            return "Error connecting to Salesforce."

        df = self.fetch_data()
        if isinstance(df, pd.DataFrame):
            return self.save_to_csv(df)
        else:
            return df  # Returning the error message from fetch_data



# Example usage:
if __name__ == "__main__":
    table_name = 'ContentVersion'  # Salesforce table (object) to query
    fields = []  # Empty list to fetch all fields
    file_name = None # No file name provided; will default to table_name.csv

    # Creating an instance of the SalesforceDataFetcher class
    fetcher = SalesforceDataFetcher()

    # Fetching data and save it to a CSV file
    result = fetcher.fetch_and_save_data(table_name, fields, file_name)
    if result:
        print(result)
