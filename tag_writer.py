import jaydebeapi
import idmc_api
import my_encrypt
import warnings
import re
warnings.filterwarnings("ignore")
'''
Make sure that your JAVA_HOME is set

'''

version = 20231202

catalog_user = 'shayes_chubb'
catalog_pass = ''
encrypted_catalog_pass = 'gAAAAABlagMSuSZbjIYsF494PJBb2ZcztqZw9zDRybNs710tn5M3Gf3a9wHc2f2oEfnMGsjF9hZIKbw51c7iYsWvxova6wwhnA=='
url_base = 'https://dmp-us.informaticacloud.com'
hawk_url_base='https://cdgc-api.dmp-us.informaticacloud.com'
asset_url_base = 'https://cdgc.dmp-us.informaticacloud.com/asset'
catalog_resource_name = 'Databricks (Bronze & Silver)'
writeback_business_term = True
writeback_business_term_tag = 'infa_business_term'
writeback_parent_policy = True
writeback_parent_policy_tag = 'infa_policy'
writeback_classification = True
writeback_classification_tag = 'infa_classification'
writeback_comment = True
include_url_in_table_comment = True
url_text = 'Open in Informatica Cloud Data Catalog'
unset_tags_first = True
writeback_tags = True

jdbc_driver = "com.databricks.client.jdbc.Driver"
jdbc_url = "jdbc:databricks://dbc-f275d626-76dc.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/80a28ffb7dbdda9f;"
jdbc_username = "token"
jdbc_password = ""
encrypted_jdbc_password = 'gAAAAABlagM1F8zQTtgGMbNIgCeSFhhCMHuAO0HenvjzCdVTaS9AzsXOHXpO9M3PcHr3MKnczVnR7HCEwaqX5rtfcHcbbe8Iqb953vHLLVEKyItIdlir-d0Gu847gq_omVSODoMJRmfc'
jdbc_driver_file = '.\DatabricksJDBC42.jar'
databricks_pre_statements = []

debugFlag=False

stop_and_verify = False




idmc_api.debugFlag=debugFlag

def debug(message):
    if debugFlag:
        print(f"DEBUG: {message}")


def execute_statement(statement):

    # Establish JDBC connection and execute query
    with jaydebeapi.connect(jdbc_driver, jdbc_url, (jdbc_username, jdbc_password), jdbc_driver_file) as conn:
        with conn.cursor() as cursor:
            cursor.execute(statement)
            result = cursor.fetchall()
            return result

'''

for row in result:
    print("----------------------------")
    print(*row)
'''
print(f"INFO: Connecting to Catalog as user {catalog_user}, and fetching some basic information")
if len(encrypted_catalog_pass) > 2:
   catalog_pass = my_encrypt.decrypt_message(encrypted_catalog_pass)  
session = idmc_api.INFASession(username=catalog_user, password=catalog_pass, url_base=url_base, hawk_url_base=hawk_url_base)


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
   jdbc_password = my_encrypt.decrypt_message(encrypted_jdbc_password) 
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

