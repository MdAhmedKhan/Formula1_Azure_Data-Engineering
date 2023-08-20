# Databricks notebook source
configs ={"fs.azure.account.auth.type":"OAuth",
"fs.azure.account.oauth.provider.type":"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "7385c400-815b-40ad-a43e-27d294c2110a",
"fs.azure.account.oauth2.client.secret": "h3-8Q~nj45Ip5BWo3A7ivbMjExPN6NEG24WoXbTY",
"fs.azure.account.oauth2.client.endpoint":"https://login.microsoftonline.com/ae145806-7b24-4ed7-b639-cb35b3addf21/oauth2/token"}




dbutils.fs.mount(
source = "abfss://raw@formulaone123.dfs.core.windows.net",
mount_point = "/mnt/formulaOne",
extra_configs= configs)


dbutils.fs.mount(
source = "abfss://processed@formulaone123.dfs.core.windows.net",
mount_point = "/mnt/formulaProcessed",
extra_configs= configs)


dbutils.fs.mount(
source = "abfss://presentation@formulaone123.dfs.core.windows.net",
mount_point = "/mnt/formulaPresentation",
extra_configs= configs)


dbutils.fs.mount(
source = "abfss://csv@formulaone123.dfs.core.windows.net",
mount_point = "/mnt/formulacsv",
extra_configs= configs)

