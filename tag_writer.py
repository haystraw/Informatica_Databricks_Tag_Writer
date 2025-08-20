import jaydebeapi
import idmc_api
import warnings
import re
import configparser
import json
import os
import sys
from cryptography.fernet import Fernet
import getpass
import argparse
import ast
from cryptography.fernet import InvalidToken
import socket
import requests
import time
warnings.filterwarnings("ignore")

help_message = '''
Make sure that your JAVA_HOME is set

Usage: Execute the script.
By default, configuration values will be read from the config.ini file.
Any parameters provided on the command line will override those in the config.ini.
Use --help to display this message.

Available parameters:

   --catalog_user
        The username to connect to the IDMC Catalog (CDGC).
        If not specified, the script will look for this in the config.ini file.
        Example:
            --catalog_user=shayes_compass

   --catalog_pass
        The password for the IDMC Catalog (CDGC) user.
        You can provide it as plain text OR as an encrypted value using --encrypted_catalog_pass.
        * See the Encryption related options to see how to create encrypted passwords
        If both are provided, the encrypted password will take precedence.
        Example:
            --catalog_pass=MySecretPassword

   --encrypted_catalog_pass
        An encrypted version of the IDMC Catalog (CDGC) password.
        * See the Encryption related options to see how to create encrypted passwords
        If provided alongside --catalog_pass, this value will be used instead.
        The script will decrypt this automatically.
        Example:
            --encrypted_catalog_pass=gAAAAABobs...

   --idmc_pod
        The IDMC pod identifier to connect to.
		If you're unsure of the pod, look at your URL for your login.
		It'll be the string just before "informaticacloud.com"
		For https://dm-us.informaticacloud.com/identity-service/home it is "dm-us"
        Example:
            --idmc_pod=dm-us

   --catalog_resource_name
        Name of the catalog resource within CDGC to target.
        Example:
            --catalog_resource_name="Azure Databricks"

   --writeback_business_term
        Boolean flag (True/False) to enable writing back business term tags.
		Do you want it to evaluate Business Terms and write these as tags to Databricks?
        Example:
            --writeback_business_term=True

   --writeback_business_term_tag
        The tag name to use when writing back business terms.
		What is the name of the Databricks tag to use?
        Example:
            --writeback_business_term_tag=infa_business_term

   --writeback_parent_policy
        Boolean flag to enable writing back parent policy tags.
		Do you want it to evaluate Policies related to Tables/Views/Columns and write these as tags to Databricks?
        Example:
            --writeback_parent_policy=True

   --writeback_parent_policy_tag
        The tag name to use when writing back parent policies.
		What is the name of the Databricks tag to use?
        Example:
            --writeback_parent_policy_tag=infa_policy

   --writeback_classification
        Boolean flag to enable writing back classification tags.
		Do you want it to evaluate classifications related to Tables/Views/Columns and write these as tags to Databricks?
        Example:
            --writeback_classification=True

   --writeback_classification_tag
        The tag name to use when writing back classifications.
		What is the name of the Databricks tag to use?
        Example:
            --writeback_classification_tag=infa_classification

   --writeback_comment
        Boolean flag to enable writing back object descriptions as comments.
		Do you want it to evaluate Descriptions related to Tables/Views/Columns and write these as comments to Databricks?
        Example:
            --writeback_comment=True

   --include_url_in_table_comment
        Boolean flag to include a URL link in the table comment when writing back.
		If evaluating comments, do you want to include a URL from the Table/View that points to the table in CDGC?
		This is particularly useful if you want your users to see additional information like lineage.
        Example:
            --include_url_in_table_comment=True

   --url_text
        The display text for the URL included in table comments.
		If including a URL, What do you want the link text to be displayed in the comment in the Table/View
        Example:
            --url_text="Open in Informatica Cloud Data Catalog"

   --unset_tags_first
        Boolean flag to unset existing tags before writing back new ones.
		Do you want be to unset these tags, before set them?
		This is useful if there may have been objects that got set, prior.
		Note it will ONLY effect the tags specified here (writeback_business_term_tag, writeback_parent_policy_tag, writeback_classification_tag)
        Example:
            --unset_tags_first=True

   --writeback_tags
        Boolean flag to enable writing back tags in general.
	Set this to False in order to test what it will do, without performing the update
        Example:
            --writeback_tags=True

   --jdbc_driver
        (Databricks JDBC) The fully qualified class name of the JDBC driver to connect to Databricks.
        Example:
            --jdbc_driver=com.databricks.client.jdbc.Driver

   --jdbc_url
        (Databricks JDBC) The JDBC connection URL string.
        Example:
            --jdbc_url="jdbc:databricks://adb-3507816793016728.8.azuredatabricks.net:443/nsen;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/endpoints/556b3ac6518656a2;"

   --jdbc_username
        (Databricks JDBC) The username for the JDBC connection.
	for Databricks, this is usually the string "token"
        Example:
            --jdbc_username=token

   --jdbc_password
        (Databricks JDBC) The password for the JDBC connection.
	For Databricks, this is typically a token that you can create.
        You can provide it as plain text OR use the encrypted version with --encrypted_jdbc_password.
	* See the Encryption related options to see how to create encrypted passwords
        If both are present, the encrypted password will be used.
        Example:
            --jdbc_password=MySecretPassword

   --encrypted_jdbc_password
        (Databricks JDBC) Encrypted JDBC password.
	* See the Encryption related options to see how to create encrypted passwords
        If provided alongside --jdbc_password, this value will be used instead.
        The script will decrypt it automatically.
        Example:
            --encrypted_jdbc_password=gAAAAABn7rHu...

   --jdbc_driver_file
        (Databricks JDBC) File path to the JDBC driver JAR file.
		You can specify just a filename (which it will expect in the same directory as the python script,
		or you can specify a full path to where the jar file resides.
        Example:
            --jdbc_driver_file=./DatabricksJDBC42.jar

   --databricks_pre_statements
        (Databricks) List of SQL statements to execute prior to main operations.
        Example:
            --databricks_pre_statements=["SET spark.sql.shuffle.partitions=10"]

   --debugFlag
        Boolean flag to enable debug mode with verbose output.
        Example:
            --debugFlag=False

   --stop_and_verify
        Boolean flag to pause execution for manual verification steps.
        Example:
            --stop_and_verify=True

Encryption related options:

   --encrypt
        Prompts to enter a plain text password and outputs its encrypted form.
        Useful for generating encrypted passwords for config or command line use.

   --decrypt
        Prompts to enter encrypted text and outputs the decrypted plain text.
        Use this to verify that encrypted passwords were copied correctly.

   --generate_key
        Generates a new secret key file (secret.key) used as the basis for encryption and decryption.
		If you generate a new secrety.key, any previous encrypted passwords will be invalidated.
        Note: This encryption is intended to obscure passwords, not provide strong cryptographic security.
'''

version = 20250820
print(f"INFO: tag_writer {version}")

script_location = os.path.dirname(os.path.abspath(sys.executable if getattr(sys, 'frozen', False) else __file__))

config = configparser.ConfigParser()
config.read(script_location+'/config.ini')

cfg = config['tag_writer']

catalog_user = cfg.get('catalog_user')
catalog_pass = cfg.get('catalog_pass')
encrypted_catalog_pass = cfg.get('encrypted_catalog_pass')
idmc_pod = cfg.get('idmc_pod')
url_base = f"https://{idmc_pod}.informaticacloud.com"
hawk_url_base = f"https://cdgc-api.{idmc_pod}.informaticacloud.com"
asset_url_base = f"https://cdgc.{idmc_pod}.informaticacloud.com/asset"

catalog_resource_name = cfg.get('catalog_resource_name')

writeback_business_term = cfg.getboolean('writeback_business_term')
writeback_business_term_tag = cfg.get('writeback_business_term_tag')
writeback_parent_policy = cfg.getboolean('writeback_parent_policy')
writeback_parent_policy_tag = cfg.get('writeback_parent_policy_tag')
writeback_classification = cfg.getboolean('writeback_classification')
writeback_classification_tag = cfg.get('writeback_classification_tag')
writeback_comment = cfg.getboolean('writeback_comment')
include_url_in_table_comment = cfg.getboolean('include_url_in_table_comment')
url_text = cfg.get('url_text')
unset_tags_first = cfg.getboolean('unset_tags_first')
writeback_tags = cfg.getboolean('writeback_tags')

jdbc_driver = cfg.get('jdbc_driver')
jdbc_url = cfg.get('jdbc_url')
jdbc_username = cfg.get('jdbc_username')
jdbc_password = cfg.get('jdbc_password')
encrypted_jdbc_password = cfg.get('encrypted_jdbc_password')
jdbc_driver_file = cfg.get('jdbc_driver_file')

debugFlag = cfg.getboolean('debugFlag')
stop_and_verify = cfg.getboolean('stop_and_verify')


# Parse databricks_pre_statements as JSON list
databricks_pre_statements = []
statements_str = cfg.get('databricks_pre_statements', '[]')
try:
    databricks_pre_statements = json.loads(statements_str)
except json.JSONDecodeError:
    pass

jdbc_driver_file_orig = jdbc_driver_file  # keep original if needed

if jdbc_driver_file.startswith('/') or ':' in jdbc_driver_file:
    # Assume full path, leave as is
    jdbc_driver_file = jdbc_driver_file
elif jdbc_driver_file.startswith('.'):
    # Replace the leading '.' with script_location
    # Example: .\DatabricksJDBC42.jar -> {script_location}\DatabricksJDBC42.jar
    jdbc_driver_file = os.path.join(script_location, jdbc_driver_file.lstrip('.\\/'))
else:
    # No '/' in path, so prefix with script_location + "/"
    if '/' not in jdbc_driver_file and '\\' not in jdbc_driver_file:
        jdbc_driver_file = os.path.join(script_location, jdbc_driver_file)
    else:
        # If path contains slashes but didn't start with '/' or '.', treat as relative and join anyway
        jdbc_driver_file = os.path.join(script_location, jdbc_driver_file)

# Optionally normalize path separators
jdbc_driver_file = os.path.normpath(jdbc_driver_file)

idmc_api.debugFlag=debugFlag

def debug(message):
    if debugFlag:
        print(f"DEBUG: {message}")

def infaLog(annotation=""):
    try:
        ## This is simply a "phone home" call.
        ## Just to note which Informatica Org is using this script
        ## If it's unable to reach this URL, it will ignore.
        this_headers = {"Content-Type": "application/json", "X-Auth-Key": "b74a58ca9f170e49f65b7c56df0f452b0861c8c870864599b2fbc656ff758f5d"}
        logs=[{"timestamp": time.time(), "function": f"[{os.path.basename(__file__)}][main]", "execution_time": "N/A", "annotation": annotation, "machine": socket.gethostname()}]
        response=requests.post("https://infa-lic-worker.tim-qin-yujue.workers.dev", data=json.dumps({"logs": logs}), headers=this_headers)
    except:
        pass

def execute_statement(statement):

    # Establish JDBC connection and execute query
    with jaydebeapi.connect(jdbc_driver, jdbc_url, (jdbc_username, jdbc_password), jdbc_driver_file) as conn:
        with conn.cursor() as cursor:
            cursor.execute(statement)
            result = cursor.fetchall()
            return result

def generate_key():
    """
    Generates a key and save it into a file
    """
    key = Fernet.generate_key()
    with open(script_location+"/secret.key", "wb") as key_file:
        key_file.write(key)

def load_key():
    """
    Load the previously generated key
    """
    return open(script_location+"/secret.key", "rb").read()

def encrypt_message(message):
    """
    Encrypts a message
    """
    key = load_key()
    encoded_message = message.encode()
    f = Fernet(key)
    encrypted_message = f.encrypt(encoded_message)

    return encrypted_message.decode()

def decrypt_message(encrypted_message):
    """
    Decrypts an encrypted message
    """
    message = encrypted_message.encode()
    key = load_key()
    f = Fernet(key)
    decrypted_message = f.decrypt(message)

    return decrypted_message.decode()

def getEncryptedString():
    string = getpass.getpass('Enter Text to encrypt:')
    encrypted = encrypt_message(string)
    print(f"Encrypted String:{encrypted}")
    '''
    decrypted = decrypt_message(encrypted)
    print(f"Decrypted: {decrypted}")
    '''
def verifyEncryption():
    string = input("Verify Encrypted String:")
    decrypted = decrypt_message(string)
    print(f"Decrypted Value: {decrypted}")

'''

for row in result:
    print("----------------------------")
    print(*row)
'''

def parse_parameters():
    # Check for specific arguments first.
        if '--help' in sys.argv:
                print(help_message)
                programPause = input("Press the <ENTER> key to exit...")
                sys.exit(0)

        if '--generate_key' in sys.argv:
                print(f"Generating new secret.key file")
                try:
                        generate_key()
                except Exception as e:
                        print(f"ERROR: Problem with the generating the secret.key. \nDetails: {e}")
                        print(f"       Details: {e}")
                        programPause = input("Press the <ENTER> key to exit...")
                        sys.exit(1)	                
                programPause = input("Press the <ENTER> key to exit...")
                sys.exit(0)                

        if '--encrypt' in sys.argv:
                print(f"Encrypting String...")
                try:
                        getEncryptedString()
                except Exception as e:
                        print(f"ERROR: Problem with the encrypting the password. \nDetails: {e}")
                        print(f"       Details: {e}")
                        print(f"       If the secret.key doesn't exist, create it using --generate_key parameter")
                        print(f"       (this will invalidate any encrypted passwords in config.ini, and you'll need to re-encrypt using --encrypt parameter)")
                        programPause = input("Press the <ENTER> key to exit...")
                        sys.exit(1)	                
                programPause = input("Press the <ENTER> key to exit...")
                sys.exit(0)                              

        if '--decrypt' in sys.argv:
                print(f"Decrypting String...")
                try:
                        verifyEncryption()
                except Exception as e:
                        print(f"ERROR: Problem with the decrypting the password. \nDetails: {e}")
                        print(f"       Details: {e}")
                        print(f"       If the secret.key doesn't exist, create it using --generate_key parameter")
                        print(f"       (this will invalidate any encrypted passwords in config.ini, and you'll need to re-encrypt using --encrypt parameter)")
                        programPause = input("Press the <ENTER> key to exit...")
                        sys.exit(1)	                
                programPause = input("Press the <ENTER> key to exit...")
                sys.exit(0) 

        parser = argparse.ArgumentParser(description="Dynamically set variables from command-line arguments.")
        args, unknown_args = parser.parse_known_args()

        for arg in unknown_args:
                if arg.startswith("--") and "=" in arg:
                        key, value = arg[2:].split("=", 1)  # Remove "--" and split into key and value
                try:
                        # Safely parse value as Python object (list, dict, etc.)
                        value = ast.literal_eval(value)
                except (ValueError, SyntaxError):
                        pass  # Leave value as-is if parsing fails

                # Handle appending to arrays or updating dictionaries
                if key in globals():
                        existing_value = globals()[key]
                        if isinstance(existing_value, list) and isinstance(value, list):
                                ## If what was passed is an array, we'll append to the array
                                existing_value.extend(value)  # Append to the existing array
                        elif isinstance(existing_value, dict) and isinstance(value, dict):
                                ## If what was passed is a dict, we'll add to the dict
                                existing_value.update(value)  # Add or update keys in the dictionary
                        else:
                                ## Otherwise, it's an ordinary variable. replace it
                                globals()[key] = value  # Replace for other types
                else:
                        ## It's a new variable. Create an ordinary variable.
                        globals()[key] = value  # Set as a new variable	


def connect_to_idmc_and_fetch_data():
    print(f"INFO: Connecting to Catalog as user {catalog_user}, and fetching some basic information")

    # Global the main variables, in case a change needs to be made
    global catalog_user, catalog_pass, encrypted_catalog_pass, idmc_pod, url_base, hawk_url_base, asset_url_base, catalog_resource_name
    global writeback_business_term, writeback_business_term_tag, writeback_parent_policy, writeback_parent_policy_tag
    global writeback_classification, writeback_classification_tag, writeback_comment, include_url_in_table_comment, url_text
    global unset_tags_first, writeback_tags, jdbc_driver, jdbc_url, jdbc_username, jdbc_password, encrypted_jdbc_password
    global jdbc_driver_file, debugFlag, stop_and_verify

    
    if len(encrypted_catalog_pass) > 2:
        try:
                catalog_pass = decrypt_message(encrypted_catalog_pass)
        except InvalidToken as e:
                print(f"ERROR: Invalid encrypted password or secret key mismatch. Please verify your encrypted password and that the secret.key file is correct.")
                print(f"       Try re-encrypting the password using the parameter --encrypt and then updating \"encrypted_catalog_pass\" in config.ini")
                print(f"       Or just removing the value for \"encrypted_catalog_pass\" from the config.ini, and using a password in clear text in \"catalog_pass\"")
                programPause = input("Press the <ENTER> key to exit...")
                sys.exit(1)
        except Exception as e:
                print(f"ERROR: Problem with the encrypted password. \nDetails: {e}")
                print(f"       Details: {e}")
                print(f"       If the secret.key doesn't exist, create it using --generate_key parameter")
                print(f"       (this will invalidate any encrypted passwords in config.ini, and you'll need to re-encrypt using --encrypt parameter)")
                programPause = input("Press the <ENTER> key to exit...")
                sys.exit(1)                
    infaLog(f"User: {catalog_user}, URL: {url_base}, Version: {version}")
    session = idmc_api.INFASession(username=catalog_user, password=catalog_pass, url_base=url_base, hawk_url_base=hawk_url_base)

    global statements, unset_statements, unset_tags
    statements = []
    unset_statements = []
    unset_tags = []

    if unset_tags_first:
        if writeback_business_term:
            unset_tags.append(writeback_business_term_tag)
        if writeback_parent_policy:
            unset_tags.append(writeback_parent_policy_tag)
        if writeback_classification:
            unset_tags.append(writeback_classification_tag) 

    for r in session.resources:
        if r.name == catalog_resource_name:
            r.fetchObjects()
            print(f"INFO: Evaluating {catalog_resource_name}")
            for obj in r.objects:
                try:
                    debug(f"Looking at path: {obj.getFriendlyId()}")
                    db_path_array = obj.getFriendlyId().split('/')
                    db_path_array.pop(0)
                    db_path_array.pop(0)
                    obj_path = ".".join(db_path_array)
                    db_path_array.pop(-1)
                    obj_parent_path = ".".join(db_path_array)
                    obj_name = obj.name
                except:
                    continue

                debug(f"Evaluating: {obj.shortType} {obj_parent_path}.{obj_name}")
                debug(f"     Classifications: {obj.getClassificationNames()}")
                debug(f"     Business Terms: {obj.getBusinessTermNames()}")

                if unset_tags_first:
                    for unset_tag in unset_tags:
                        if obj.shortType.endswith('ViewColumn'):
                            unset_statement = f"ALTER VIEW {obj_parent_path} ALTER Column {obj_name} UNSET tags ( '{unset_tag}' )"
                            unset_statements.append(unset_statement)
                            debug(f"Adding unset statement: {unset_statement}")                        
                        elif obj.shortType.endswith('Column'):
                            unset_statement = f"ALTER TABLE {obj_parent_path} ALTER Column {obj_name} UNSET tags ( '{unset_tag}' )"
                            unset_statements.append(unset_statement)
                            debug(f"Adding unset statement: {unset_statement}")
                        elif obj.shortType.endswith('Table'):
                            unset_statement = f"ALTER TABLE {obj_parent_path}.{obj_name} UNSET tags ( '{unset_tag}' )"
                            unset_statements.append(unset_statement)
                            debug(f"Adding unset statement: {unset_statement}")
                        elif obj.shortType.endswith('View'):
                            unset_statement = f"ALTER VIEW {obj_parent_path}.{obj_name} UNSET tags ( '{unset_tag}' )"
                            unset_statements.append(unset_statement)
                            debug(f"Adding unset statement: {unset_statement}")                        

                if len(obj.getBusinessTermNames()) > 0 and writeback_business_term:
                    if obj.shortType.endswith('ViewColumn'):
                        statement = "ALTER VIEW "+obj_parent_path+" ALTER Column "+obj_name+" SET tags ( '"+writeback_business_term_tag+"' = '"+obj.getBusinessTermNames()+"')"
                        statements.append(statement)
                        print("INFO: Adding "+statement)                 
                    elif obj.shortType.endswith('Column'):
                        statement = "ALTER TABLE "+obj_parent_path+" ALTER Column "+obj_name+" SET tags ( '"+writeback_business_term_tag+"' = '"+obj.getBusinessTermNames()+"' )"
                        statements.append(statement)
                        print("INFO: Adding "+statement)
                    elif obj.shortType.endswith('Table'):
                        statement = "ALTER TABLE "+obj_parent_path+"."+obj_name+" SET tags ( '"+writeback_business_term_tag+"' = '"+obj.getBusinessTermNames()+"')"
                        statements.append(statement)
                        print("INFO: Adding "+statement)
                    elif obj.shortType.endswith('View'):
                        statement = "ALTER VIEW "+obj_parent_path+"."+obj_name+" SET tags ( '"+writeback_business_term_tag+"' = '"+obj.getBusinessTermNames()+"')"
                        statements.append(statement)
                        print("INFO: Adding "+statement)                        

                if len(obj.getParentPolicyNames()) > 1 and writeback_parent_policy:
                    if obj.shortType.endswith('ViewColumn'):
                        statement = "ALTER VIEW "+obj_parent_path+" ALTER Column "+obj_name+" SET tags ( '"+writeback_parent_policy_tag+"' = '"+obj.getParentPolicyNames()+"')"
                        statements.append(statement)
                        print("INFO: Adding "+statement)                  
                    elif obj.shortType.endswith('Column'):
                        statement = "ALTER TABLE "+obj_parent_path+" ALTER Column "+obj_name+" SET tags ( '"+writeback_parent_policy_tag+"' = '"+obj.getParentPolicyNames()+"')"
                        statements.append(statement)
                        print("INFO: Adding "+statement)
                    elif obj.shortType.endswith('Table'):
                        statement = "ALTER TABLE "+obj_parent_path+"."+obj_name+" SET tags ( '"+writeback_parent_policy_tag+"' = '"+obj.getParentPolicyNames()+"')"
                        statements.append(statement)
                        print("INFO: Adding "+statement)
                    elif obj.shortType.endswith('View'):
                        statement = "ALTER VIEW "+obj_parent_path+"."+obj_name+" SET tags ( '"+writeback_parent_policy_tag+"' = '"+obj.getParentPolicyNames()+"')"
                        statements.append(statement)
                        print("INFO: Adding "+statement)                        

                if len(obj.getClassificationNames()) > 0 and writeback_classification:
                    if obj.shortType.endswith('ViewColumn'):
                        statement = "ALTER VIEW "+obj_parent_path+" ALTER Column "+obj_name+" SET tags ( '"+writeback_classification_tag+"' = '"+obj.getClassificationNames()+"')"
                        statements.append(statement)
                        print("INFO: Adding "+statement)                 
                    elif obj.shortType.endswith('Column'):
                        statement = "ALTER TABLE "+obj_parent_path+" ALTER Column "+obj_name+" SET tags ( '"+writeback_classification_tag+"' = '"+obj.getClassificationNames()+"')"
                        statements.append(statement)
                        print("INFO: Adding "+statement)
                    elif obj.shortType.endswith('Table'):
                        statement = "ALTER TABLE "+obj_parent_path+"."+obj_name+" SET tags ( '"+writeback_classification_tag+"' = '"+obj.getClassificationNames()+"')"
                        statements.append(statement)                        
                        print("INFO: Adding "+statement)
                    elif obj.shortType.endswith('View'):
                        statement = "ALTER VIEW "+obj_parent_path+"."+obj_name+" SET tags ( '"+writeback_classification_tag+"' = '"+obj.getClassificationNames()+"')"
                        statements.append(statement)
                        print("INFO: Adding "+statement)                        

                if writeback_comment:
                    if obj.shortType.endswith('ViewColumn') and ( len(obj.description) > 2 or include_url_in_table_comment ):
                        description_1 = re.sub('<[^<]+?>', '', obj.description)
                        description_2 = description_1.replace("'", "\\'")                        
                        statement = f"alter view {obj_parent_path} alter column {obj_name} comment '{description_2}'"
                        statements.append(statement)
                        print("INFO: Adding "+statement) 
                    elif obj.shortType.endswith('Column') and len(obj.description) > 2:
                        description_1 = re.sub('<[^<]+?>', '', obj.description)
                        description_2 = description_1.replace("'", "\\'")                        
                        statement = f"alter table {obj_parent_path} alter column {obj_name} comment '{description_2}'"
                        statements.append(statement)
                        print("INFO: Adding "+statement)   
                    elif obj.shortType.endswith('View') and ( len(obj.description) > 2 or include_url_in_table_comment ):
                        description_1 = re.sub('<[^<]+?>', '', obj.description)
                        description_2 = description_1.replace("'", "\\'")
                        if include_url_in_table_comment:
                            description_2 = f"{description_2}   ([{url_text}]({asset_url_base}/{obj.identity}))"
                        statement = f"COMMENT ON VIEW {obj_parent_path}.{obj_name} is '{description_2}'"
                        statements.append(statement)
                        print("INFO: Adding "+statement)                                                            
                    elif obj.shortType.endswith('Table') and ( len(obj.description) > 2 or include_url_in_table_comment ):
                        description_1 = re.sub('<[^<]+?>', '', obj.description)
                        description_2 = description_1.replace("'", "\\'")
                        if include_url_in_table_comment:
                            description_2 = f"{description_2}   ([{url_text}]({asset_url_base}/{obj.identity}))"
                        statement = f"COMMENT ON TABLE {obj_parent_path}.{obj_name} is '{description_2}'"
                        statements.append(statement)
                        print("INFO: Adding "+statement)




def connect_to_databricks_and_update():
    if unset_tags_first:
        print(f"INFO: Will unset Tags")
    else: 
        print(f"INFO: Will NOT unset Tags")
    if writeback_tags:
        print(f"INFO: Will update Tags")
    else:
        print(f"INFO: Will NOT update Tags")
    if stop_and_verify:
        input("Press any key to continue...")

    if len(encrypted_jdbc_password) > 2:
        try:
                jdbc_password = decrypt_message(encrypted_jdbc_password) 
        except InvalidToken as e:
                print(f"ERROR: Invalid encrypted password or secret key mismatch. Please verify your encrypted password and that the secret.key file is correct.")
                print(f"       Try re-encrypting the password using the parameter --encrypt and then updating \"encrypted_jdbc_password\" in config.ini")
                print(f"       Or just removing the value for \"encrypted_jdbc_password\" from the config.ini, and using a password in clear text in \"jdbc_password\"")
                programPause = input("Press the <ENTER> key to exit...")
                sys.exit(1)
        except Exception as e:
                print(f"ERROR: Problem with the encrypted password. \nDetails: {e}")
                print(f"       Details: {e}")
                print(f"       if the secret.key doesn't exist, create it using --generate_key parameter")
                print(f"       (this will invalidate any encrypted passwords in config.ini, and you'll need to re-encrypt using --encrypt parameter)")
                programPause = input("Press the <ENTER> key to exit...")
                sys.exit(1)            
        
    with jaydebeapi.connect(jdbc_driver, jdbc_url, (jdbc_username, jdbc_password), jdbc_driver_file) as conn:
        with conn.cursor() as cursor:
            print("INFO: Executing pre-statements")
            for pre_statement in databricks_pre_statements:
                cursor.execute(pre_statement)
            
            if unset_tags_first:
                print(f"INFO: Executing statements to unset these tags: {','.join(unset_tags)}")
                for unset_statement in unset_statements:
                    try:
                            cursor.execute(unset_statement)            
                    except Exception as e:
                            message = str(e)
                            ## If I try to alter tags on the built in catalog, ignore
                            if "built-in catalogs" in message:
                                    pass
            if writeback_tags:
                for statement in statements:
                    print("INFO: Executing "+statement)
                    try:
                            cursor.execute(statement)            
                    except Exception as e:
                            message = str(e)
                            ## If I try to alter tags on the built in catalog, ignore
                            if "built-in catalogs" in message:
                                    pass
                            
if __name__ == "__main__":
        parse_parameters()
        debug("Configuration Variables:")
        for var_name in [
        'catalog_user', 'catalog_pass', 'encrypted_catalog_pass', 'idmc_pod', 'catalog_resource_name',
        'writeback_business_term', 'writeback_business_term_tag', 'writeback_parent_policy', 'writeback_parent_policy_tag',
        'writeback_classification', 'writeback_classification_tag', 'writeback_comment', 'include_url_in_table_comment',
        'url_text', 'unset_tags_first', 'writeback_tags', 'jdbc_driver', 'jdbc_url', 'jdbc_username', 'jdbc_password',
        'encrypted_jdbc_password', 'jdbc_driver_file', 'databricks_pre_statements', 'debugFlag', 'stop_and_verify'
        ]:
                debug(f"    {var_name} = {repr(eval(var_name))}")    
        
        java_home = os.environ.get('JAVA_HOME')
        if not java_home:
                print("ERROR: The environment variable JAVA_HOME is not set. Please set JAVA_HOME to your Java installation directory.")
                programPause = input("Press the <ENTER> key to exit...")
                sys.exit(1)        
        connect_to_idmc_and_fetch_data()                
        connect_to_databricks_and_update()
        programPause = input("Press the <ENTER> key to exit...")
        sys.exit(0)
