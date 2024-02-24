###########################################################################
### Steps
### 1. Save 1.html in Azure storage
### 2. Run the below code which saves 2.html file in Azure Storage
############################################################################

%python

import pandas as pd

############################################################################################
### Python function - Start
############################################################################################
def unmount(remount_flag, unmount_path):
  if remount_flag:
    if any(mount.mountPoint == unmount_path for mount in dbutils.fs.mounts()):
      print("Un-mounting mount point " + unmount_path)
      dbutils.fs.unmount(unmount_path)
      print("Mountpoint " + unmount_path + " unmounted successfully")

def mount(mount_path):
  if not any(mount.mountPoint == mount_path for mount in dbutils.fs.mounts()):
    print("mounting mount point =" + mount_path)
    print("Blob container = " + blob_container)
    print("Storage Account = " + storage_account)

    dbutils.fs.mount(
      source="wasbs://" + blob_container + "@" + storage_account + ".blob.core.windows.net/" + blob_folder + "/",
      mount_point=mount_path,
      extra_configs={"fs.azure.sas." + blob_container + "." + storage_account + ".blob.core.windows.net": dbutils.secrets.get(scope=vault_scope, key=storage_account_sas_key)})
    print("Mount Path  => " + mount_path + " mounting completed")
  else:
    print("Mount Path => " + mount_path + " already mounted")

def move_and_delete_files(mnt_path: str, mnt_part_path: str, file_name: str):
  print("mnt_path :" +mnt_path)
  print("mnt_part_path :" +mnt_part_path)
  print("file_name :" +file_name)

  files = dbutils.fs.ls(mnt_part_path)
  txt_files = filter(lambda f: f.name.endswith(".txt"), files)
    
  for file in txt_files:
    source_path = mnt_part_path + "/" + file.name
    target_path = mnt_path +"/"+ file_name
    print(f"source_path = {source_path}")
    print(f"target_path = {target_path}")
      
    try:
      dbutils.fs.mv(source_path, target_path)
      print(f"Successfully moved {source_path} to {target_path}")
      if dbutils.fs.ls(mnt_part_path):
        dbutils.fs.rm(mnt_part_path, recurse=True)
    except Exception as e:
        print(f"Error moving {source_path}: {e}")
  
############################################################################################
### Python function - End
############################################################################################


############################################################################################
### Python - Feature Code - Start
############################################################################################

### 01 -  variables - storage + env
storage_account = "strg-name"
blob_container = "cntnr-name"
blob_folder = "blob-test/html"
env = "tlab"

### 02 -  variables - vault + storage access keys
storage_account_sas_key = "storage-adls-sas-key"
vault_scope = "az-vault-v0"

### 03 - variables - mount
mount_path = "/mnt/t01"
remount_flag = False

### 04 - do un-mount & mount 
unmount(remount_flag, mount_path)
mount(mount_path)

### 05 - Create DataFrame 
df = spark.sql("SELECT * from db_schema.table_name")

### 06 - Convert the DataFrame to a Pandas DataFrame & to html
pandas_df = df.toPandas()
html_table = pandas_df.to_html()

### 07 - Check if the file exists before opening it
blob_name = "1.html"
file_path = f"{mount_path}/{blob_name}"
html_df = spark.read.text(file_path, wholetext=True)
html_content = '\r\n'.join(html_df.toPandas()["value"]).encode('utf8')
html_content_str = html_content.decode('utf8')
new_string = html_content_str.replace("{html_table}", html_table)

# Convert the modified string to a DataFrame
df01 = pd.DataFrame({"HTML": [new_string]})
df02 = spark_df01.select("HTML")

### write file to storage
file_path = f"{mount_path}/part"
df02.coalesce(1).write.mode("overwrite").text(file_path)
move_and_delete_files(mount_path, (mount_path+"/part"), "2.html")

