# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://sales@satish22.blob.core.windows.net",
  mount_point = "/mnt/satish22",
  extra_configs = {"fs.azure.account.key.satish22.blob.core.windows.net":"eMvY0GFTsqVmmU/XTndcS3xdH5B8Rx97l6Y3NtQWgkZcIs1ushyAl9OZVNFfHbBnCUQEDZ6dY22C+AStxkRWfg=="})
