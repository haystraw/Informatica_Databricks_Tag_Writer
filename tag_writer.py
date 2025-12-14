import idmc_api
import warnings
import re
import configparser
import json
import os
import sys
from cryptography.fernet import Fernet, InvalidToken
import getpass
import argparse
import ast
import socket
import requests
import time
from databricks import sql

warnings.filterwarnings("ignore")

help_message = '''
Make sure you have Python 3.7+ installed.

Usage: Execute the script.
Configuration values will be read from config.ini by default.
Command-line parameters override config.ini.
Use --help to display this message.

Available parameters:

   --catalog_user
        The username to connect to the IDMC Catalog (CDGC).
        Example:
            --catalog_user=shayes_compass

   --catalog_pass
        The password for the IDMC Catalog user (unencrypted).
        Example:
            --catalog_pass=MySecretPassword

   --encrypted_catalog_pass
        Encrypted password for the IDMC Catalog user.
        Example:
            --encrypted_catalog_pass=gAAAAABo...

   --idmc_pod
        The IDMC pod identifier.
        Example:
            --idmc_pod=dmp-us

   --catalog_resource_name
        Name of the catalog resource within CDGC to target.
        Example:
            --catalog_resource_name="Azure Databricks"

   --writeback_business_term
        Boolean flag to enable writing back business term tags.
        Example:
            --writeback_business_term=True

   --writeback_business_term_tag
        The tag name to use when writing back business terms.
        Example:
            --writeback_business_term_tag=infa_business_term

   --writeback_parent_policy
        Boolean flag to enable writing back parent policy tags.
        Example:
            --writeback_parent_policy=True

   --writeback_parent_policy_tag
        Tag name to use when writing back parent policies.
        Example:
            --writeback_parent_policy_tag=infa_policy

   --writeback_classification
        Boolean flag to enable writing back classification tags.
        Example:
            --writeback_classification=True

   --writeback_classification_tag
        Tag name to use when writing back classifications.
        Example:
            --writeback_classification_tag=infa_classification

   --writeback_comment
        Boolean flag to enable writing back object descriptions as comments.
        Example:
            --writeback_comment=True

   --include_url_in_table_comment
        Boolean flag to include a URL link in the table comment when writing back.
        Example:
            --include_url_in_table_comment=True

   --url_text
        The display text for the included URL in comments.
        Example:
            --url_text="Open in Informatica Cloud Data Catalog"

   --unset_tags_first
        Boolean flag to unset existing tags before writing new ones.
        Example:
            --unset_tags_first=True

   --writeback_tags
        Boolean flag to enable writing back tags.
        Set False to test without performing changes.
        Example:
            --writeback_tags=True

   --databricks_hostname
        Databricks server hostname (required if databricks_http_path specified).
        Example:
            --databricks_hostname=adb-3507816793016728.8.azuredatabricks.net

   --databricks_port
        Optional port for Databricks server (default 443).
        Example:
            --databricks_port=443

   --databricks_http_path
        Databricks HTTP Path identifying the SQL endpoint.
        Required if databricks_hostname is specified.
        Example:
            --databricks_http_path=/sql/1.0/endpoints/556b3ac6518656a2

   --token_name
        The name of the authentication token (usually "token").
        Example:
            --token_name=token

   --token_value
        Personal access token in plain text.
        Example:
            --token_value=dapi12345...

   --encrypted_token_value
        Encrypted personal access token.
        Example:
            --encrypted_token_value=gAAAAABn7rHu...

   --databricks_pre_statements
        JSON list of SQL statements to execute prior to main operations.
        Example:
            --databricks_pre_statements=["SET spark.sql.shuffle.partitions=10"]

   --debugFlag
        Enable debug mode with verbose output.
        Example:
            --debugFlag=False

   --stop_and_verify
        Pause execution for manual verification.
        Example:
            --stop_and_verify=True

Encryption options:

   --encrypt
        Prompt for a plain text password and output encrypted string.
        Example:
            --encrypt

   --decrypt
        Prompt for an encrypted string and output decrypted password.
        Example:
            --decrypt

   --generate_key
        Generate a new encryption secret.key file (invalidates previous encrypted passwords).
        Example:
            --generate_key
'''

version = 20251214
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

jdbc_url = cfg.get('jdbc_url', fallback='')

databricks_hostname = cfg.get('databricks_hostname', fallback='')
databricks_port = cfg.get('databricks_port', fallback='443')
if databricks_port == '':
    databricks_port = '443'

databricks_http_path = cfg.get('databricks_http_path', fallback='')

token_name = cfg.get('token_name', fallback='token')
token_value = cfg.get('token_value', fallback='')
encrypted_token_value = cfg.get('encrypted_token_value', fallback='')

databricks_pre_statements = []
statements_str = cfg.get('databricks_pre_statements', '[]')
try:
    databricks_pre_statements = json.loads(statements_str)
except json.JSONDecodeError:
    pass

debugFlag = cfg.getboolean('debugFlag')
stop_and_verify = cfg.getboolean('stop_and_verify')

idmc_api.debugFlag = debugFlag

def debug(message):
    if debugFlag:
        print(f"DEBUG: {message}")

def infaLog(annotation=""):
    try:
        this_headers = {"Content-Type": "application/json",
                        "X-Auth-Key": "b74a58ca9f170e49f65b7c56df0f452b0861c8c870864599b2fbc656ff758f5d"}
        logs = [{"timestamp": time.time(), "function": f"[{os.path.basename(__file__)}][main]", "execution_time": "N/A",
                 "annotation": annotation, "machine": socket.gethostname()}]
        requests.post("https://infa-lic-worker.tim-qin-yujue.workers.dev", data=json.dumps({"logs": logs}),
                      headers=this_headers)
    except:
        pass

def generate_key():
    key = Fernet.generate_key()
    with open(script_location + "/secret.key", "wb") as key_file:
        key_file.write(key)

def load_key():
    return open(script_location + "/secret.key", "rb").read()

def encrypt_message(message):
    key = load_key()
    f = Fernet(key)
    encrypted_message = f.encrypt(message.encode())
    return encrypted_message.decode()

def decrypt_message(encrypted_message):
    key = load_key()
    f = Fernet(key)
    decrypted_message = f.decrypt(encrypted_message.encode())
    return decrypted_message.decode()

def getEncryptedString():
    string = getpass.getpass('Enter Text to encrypt:')
    encrypted = encrypt_message(string)
    print(f"Encrypted String:{encrypted}")

def verifyEncryption():
    string = input("Verify Encrypted String:")
    decrypted = decrypt_message(string)
    print(f"Decrypted Value: {decrypted}")

def parse_parameters():
    if '--help' in sys.argv:
        print(help_message)
        input("Press <ENTER> to exit...")
        sys.exit(0)

    if '--generate_key' in sys.argv:
        print("Generating new secret.key file")
        try:
            generate_key()
        except Exception as e:
            print(f"ERROR generating secret.key:\n{e}")
            input("Press <ENTER> to exit...")
            sys.exit(1)
        input("Press <ENTER> to exit...")
        sys.exit(0)

    if '--encrypt' in sys.argv:
        print("Encrypting String...")
        try:
            getEncryptedString()
        except Exception as e:
            print(f"ERROR encrypting password:\n{e}")
            input("Press <ENTER> to exit...")
            sys.exit(1)
        input("Press <ENTER> to exit...")
        sys.exit(0)

    if '--decrypt' in sys.argv:
        print("Decrypting String...")
        try:
            verifyEncryption()
        except Exception as e:
            print(f"ERROR decrypting password:\n{e}")
            input("Press <ENTER> to exit...")
            sys.exit(1)
        input("Press <ENTER> to exit...")
        sys.exit(0)

    parser = argparse.ArgumentParser(description="Dynamically set variables from command-line arguments.")
    args, unknown_args = parser.parse_known_args()

    for arg in unknown_args:
        if arg.startswith("--") and "=" in arg:
            key, value = arg[2:].split("=", 1)
            try:
                value = ast.literal_eval(value)
            except (ValueError, SyntaxError):
                pass
            globals()[key] = value

def build_hostname_and_http_path():
    # Use databricks_hostname & databricks_http_path if provided, else parse jdbc_url
    if databricks_hostname and databricks_http_path:
        return databricks_hostname, databricks_http_path
    elif jdbc_url:
        host_match = re.search(r"jdbc:databricks://([^:;/]+)", jdbc_url)
        path_match = re.search(r"httpPath=([^;]+)", jdbc_url)
        if not host_match or not path_match:
            raise ValueError("Invalid jdbc_url format for hostname and httpPath")
        return host_match.group(1), path_match.group(1)
    else:
        raise ValueError("Must specify either databricks_hostname and databricks_http_path, or provide valid jdbc_url in config")

def get_access_token():
    if encrypted_token_value and len(encrypted_token_value) > 2:
        try:
            return decrypt_message(encrypted_token_value)
        except InvalidToken:
            print("ERROR: Invalid encrypted token or secret key mismatch. Please re-encrypt the token.")
            input("Press <ENTER> to exit...")
            sys.exit(1)
        except Exception as e:
            print(f"ERROR decrypting token: {e}")
            input("Press <ENTER> to exit...")
            sys.exit(1)
    elif token_value:
        return token_value
    else:
        print("ERROR: No token_value or encrypted_token_value provided for Databricks authentication.")
        input("Press <ENTER> to exit...")
        sys.exit(1)

def execute_statement(statement, hostname, http_path, access_token):
    with sql.connect(
        server_hostname=hostname,
        http_path=http_path,
        access_token=access_token
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(statement)
            try:
                return cursor.fetchall()
            except Exception:
                # no result to fetch, e.g. for ALTER statements
                return None

def connect_to_idmc_and_fetch_data():
    global catalog_pass

    print(f"INFO: Connecting to Catalog as user {catalog_user}, and fetching some basic information")

    if encrypted_catalog_pass and len(encrypted_catalog_pass) > 2:
        try:
            catalog_pass = decrypt_message(encrypted_catalog_pass)
        except InvalidToken:
            print("ERROR: Invalid encrypted catalog password or secret key mismatch. Please re-encrypt.")
            input("Press <ENTER> to exit...")
            sys.exit(1)
        except Exception as e:
            print(f"ERROR decrypting catalog password: {e}")
            input("Press <ENTER> to exit...")
            sys.exit(1)

    infaLog(f"User: {catalog_user}, URL: {url_base}, Version: {version}")

    session = idmc_api.INFASession(username=catalog_user, password=catalog_pass, url_base=url_base,
                                   hawk_url_base=hawk_url_base)

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
                        statement = f"ALTER VIEW {obj_parent_path} ALTER Column {obj_name} SET tags ( '{writeback_business_term_tag}' = '{obj.getBusinessTermNames()}' )"
                        statements.append(statement)
                        print(f"INFO: Adding {statement}")
                    elif obj.shortType.endswith('Column'):
                        statement = f"ALTER TABLE {obj_parent_path} ALTER Column {obj_name} SET tags ( '{writeback_business_term_tag}' = '{obj.getBusinessTermNames()}' )"
                        statements.append(statement)
                        print(f"INFO: Adding {statement}")
                    elif obj.shortType.endswith('Table'):
                        statement = f"ALTER TABLE {obj_parent_path}.{obj_name} SET tags ( '{writeback_business_term_tag}' = '{obj.getBusinessTermNames()}' )"
                        statements.append(statement)
                        print(f"INFO: Adding {statement}")
                    elif obj.shortType.endswith('View'):
                        statement = f"ALTER VIEW {obj_parent_path}.{obj_name} SET tags ( '{writeback_business_term_tag}' = '{obj.getBusinessTermNames()}' )"
                        statements.append(statement)
                        print(f"INFO: Adding {statement}")

                if len(obj.getParentPolicyNames()) > 1 and writeback_parent_policy:
                    if obj.shortType.endswith('ViewColumn'):
                        statement = f"ALTER VIEW {obj_parent_path} ALTER Column {obj_name} SET tags ( '{writeback_parent_policy_tag}' = '{obj.getParentPolicyNames()}' )"
                        statements.append(statement)
                        print(f"INFO: Adding {statement}")
                    elif obj.shortType.endswith('Column'):
                        statement = f"ALTER TABLE {obj_parent_path} ALTER Column {obj_name} SET tags ( '{writeback_parent_policy_tag}' = '{obj.getParentPolicyNames()}' )"
                        statements.append(statement)
                        print(f"INFO: Adding {statement}")
                    elif obj.shortType.endswith('Table'):
                        statement = f"ALTER TABLE {obj_parent_path}.{obj_name} SET tags ( '{writeback_parent_policy_tag}' = '{obj.getParentPolicyNames()}' )"
                        statements.append(statement)
                        print(f"INFO: Adding {statement}")
                    elif obj.shortType.endswith('View'):
                        statement = f"ALTER VIEW {obj_parent_path}.{obj_name} SET tags ( '{writeback_parent_policy_tag}' = '{obj.getParentPolicyNames()}' )"
                        statements.append(statement)
                        print(f"INFO: Adding {statement}")

                if len(obj.getClassificationNames()) > 0 and writeback_classification:
                    if obj.shortType.endswith('ViewColumn'):
                        statement = f"ALTER VIEW {obj_parent_path} ALTER Column {obj_name} SET tags ( '{writeback_classification_tag}' = '{obj.getClassificationNames()}' )"
                        statements.append(statement)
                        print(f"INFO: Adding {statement}")
                    elif obj.shortType.endswith('Column'):
                        statement = f"ALTER TABLE {obj_parent_path} ALTER Column {obj_name} SET tags ( '{writeback_classification_tag}' = '{obj.getClassificationNames()}' )"
                        statements.append(statement)
                        print(f"INFO: Adding {statement}")
                    elif obj.shortType.endswith('Table'):
                        statement = f"ALTER TABLE {obj_parent_path}.{obj_name} SET tags ( '{writeback_classification_tag}' = '{obj.getClassificationNames()}' )"
                        statements.append(statement)
                        print(f"INFO: Adding {statement}")
                    elif obj.shortType.endswith('View'):
                        statement = f"ALTER VIEW {obj_parent_path}.{obj_name} SET tags ( '{writeback_classification_tag}' = '{obj.getClassificationNames()}' )"
                        statements.append(statement)
                        print(f"INFO: Adding {statement}")

                if writeback_comment:
                    if obj.shortType.endswith('ViewColumn') and (len(obj.description) > 2 or include_url_in_table_comment):
                        description_1 = re.sub('<[^<]+?>', '', obj.description)
                        description_2 = description_1.replace("'", "\\'")
                        statement = f"alter view {obj_parent_path} alter column {obj_name} comment '{description_2}'"
                        statements.append(statement)
                        print(f"INFO: Adding {statement}")
                    elif obj.shortType.endswith('Column') and len(obj.description) > 2:
                        description_1 = re.sub('<[^<]+?>', '', obj.description)
                        description_2 = description_1.replace("'", "\\'")
                        statement = f"alter table {obj_parent_path} alter column {obj_name} comment '{description_2}'"
                        statements.append(statement)
                        print(f"INFO: Adding {statement}")
                    elif obj.shortType.endswith('View') and (len(obj.description) > 2 or include_url_in_table_comment):
                        description_1 = re.sub('<[^<]+?>', '', obj.description)
                        description_2 = description_1.replace("'", "\\'")
                        if include_url_in_table_comment:
                            description_2 = f"{description_2}   ([{url_text}]({asset_url_base}/{obj.identity}))"
                        statement = f"COMMENT ON VIEW {obj_parent_path}.{obj_name} is '{description_2}'"
                        statements.append(statement)
                        print(f"INFO: Adding {statement}")
                    elif obj.shortType.endswith('Table') and (len(obj.description) > 2 or include_url_in_table_comment):
                        description_1 = re.sub('<[^<]+?>', '', obj.description)
                        description_2 = description_1.replace("'", "\\'")
                        if include_url_in_table_comment:
                            description_2 = f"{description_2}   ([{url_text}]({asset_url_base}/{obj.identity}))"
                        statement = f"COMMENT ON TABLE {obj_parent_path}.{obj_name} is '{description_2}'"
                        statements.append(statement)
                        print(f"INFO: Adding {statement}")

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

    hostname_to_use, http_path_to_use = build_hostname_and_http_path()
    access_token = get_access_token()

    print("INFO: Executing pre-statements")
    for pre_statement in databricks_pre_statements:
        execute_statement(pre_statement, hostname_to_use, http_path_to_use, access_token)

    if unset_tags_first:
        print(f"INFO: Executing statements to unset these tags: {','.join(unset_tags)}")
        for unset_statement in unset_statements:
            try:
                execute_statement(unset_statement, hostname_to_use, http_path_to_use, access_token)
            except Exception as e:
                message = str(e)
                if "built-in catalogs" in message:
                    pass

    if writeback_tags:
        for statement in statements:
            print("INFO: Executing " + statement)
            try:
                execute_statement(statement, hostname_to_use, http_path_to_use, access_token)
            except Exception as e:
                message = str(e)
                if "built-in catalogs" in message:
                    pass



if __name__ == "__main__":
    parse_parameters()
    debug("Configuration Variables:")
    for var_name in [
        'catalog_user', 'catalog_pass', 'encrypted_catalog_pass', 'idmc_pod', 'catalog_resource_name',
        'writeback_business_term', 'writeback_business_term_tag', 'writeback_parent_policy', 'writeback_parent_policy_tag',
        'writeback_classification', 'writeback_classification_tag', 'writeback_comment', 'include_url_in_table_comment',
        'url_text', 'unset_tags_first', 'writeback_tags', 'databricks_hostname', 'databricks_port', 'databricks_http_path',
        'token_name', 'token_value', 'encrypted_token_value', 'databricks_pre_statements', 'debugFlag', 'stop_and_verify'
    ]:
        debug(f"    {var_name} = {repr(eval(var_name))}")

    connect_to_idmc_and_fetch_data()
    connect_to_databricks_and_update()
    input("Press the <ENTER> key to exit...")
    sys.exit(0)
